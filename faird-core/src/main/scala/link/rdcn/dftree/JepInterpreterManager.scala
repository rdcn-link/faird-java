package link.rdcn.dftree

import jep._
import link.rdcn.{ConfigLoader, Logging}

import java.nio.file.{Files, Paths}
import scala.sys.process._
/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/21 18:30
 * @Modified By:
 */

object JepInterpreterManager extends Logging{
  // ThreadLocal stores each thread's SharedInterpreter instance
  private val threadLocalInterpreter: ThreadLocal[SharedInterpreter] = new ThreadLocal[SharedInterpreter] {
    override def initialValue(): SharedInterpreter = {
      logger.debug(s"Initializing SharedInterpreter for thread: ${Thread.currentThread().getName}")
      new SharedInterpreter()
    }
  }

  // Get the SharedInterpreter instance for the current thread
  def getInterpreter: SharedInterpreter = threadLocalInterpreter.get()

  // Close the SharedInterpreter instance for the current thread.
  // IMPORTANT: This should be called when the thread finishes its lifecycle or no longer needs Jep.
  def closeInterpreterForCurrentThread(): Unit = {
    val interp = threadLocalInterpreter.get()
    if (interp != null) {
      try {
        // --- REMOVED THE isClosed CHECK HERE ---
        interp.close()
        logger.debug(s"SharedInterpreter for thread ${Thread.currentThread().getName} has been closed.")
      } catch {
        case e: JepException if e.getMessage.contains("already closed") =>
          // Catch and log if it's already closed. This might happen if cleanup logic is called multiple times.
          logger.warn(s"Warning: Interpreter for thread ${Thread.currentThread().getName} already closed, ignoring. ${e.getMessage}")
        case e: Exception =>
          logger.error(s"Error closing interpreter for thread ${Thread.currentThread().getName}: ${e.getMessage}")
      } finally {
        threadLocalInterpreter.remove() // Remove from ThreadLocal to prevent memory leaks and ensure new interp on next get()
      }
    }
  }
  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    logger.debug("JVM shutdown hook activated. Attempting to close current thread's Jep interpreter if active.")
    closeInterpreterForCurrentThread()
  }))

  def getJepInterpreter(functionId: String, whlPath: String): SubInterpreter = {
    //根据functionId 下载.whl包 对接算子库获取，构造依赖环境
    try{
      val sitePackagePath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "python", functionId).toString
      //将依赖环境安装到指定目录
      val env = Option(ConfigLoader.fairdConfig.pythonHome).map("PATH" -> _)
        .getOrElse("PATH" -> sys.env.getOrElse("PATH", ""))
      val cmd = Seq(getPythonExecutablePath(env._2), "-m", "pip", "install", "--upgrade", "--target", sitePackagePath, whlPath)
      val output = Process(cmd, None, env).!!
      logger.debug(output)
      logger.debug(s"Python dependency from '$functionId' has been successfully installed to '$sitePackagePath'.")
      val config = new JepConfig
      config.addIncludePaths(sitePackagePath).setClassEnquirer(new JavaUtilOnlyClassEnquirer)
      new SubInterpreter(config)
    }catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }

  }

  private def getPythonExecutablePath(env: String): String = {
    // 1. 获取当前进程的 PATH 环境变量，考虑 PYTHONHOME 作为备选
    // 2. 尝试从 CONDA_PREFIX 构造 Anaconda 环境的 bin/Scripts 目录，并添加到 PATH 前面
    val condaPrefixPath = sys.env.get("CONDA_PREFIX") match {
      case Some(prefix) =>
        // Windows 上通常是 envs/name/Scripts 和 envs/name
        // Linux/macOS 上是 envs/name/bin
        val binPath = Paths.get(prefix, "bin").toString
        val scriptsPath = Paths.get(prefix, "Scripts").toString // Windows 特有
        // 确保路径不重复
        Seq(binPath, scriptsPath).filter(p => Files.exists(Paths.get(p)))
      case None => Seq.empty[String]
    }

    // 3. 将 PATH 字符串拆分为单独的目录，并去重
    // Windows 路径分隔符是 ';', Unix-like 是 ':'
    val pathSeparator = sys.props("path.separator")
    val pathDirs = (env.split(pathSeparator) ++ condaPrefixPath).distinct // 合并并去重

    // 4. 尝试在这些目录中查找 python.exe 或 python3.exe
//    val commonPythonExecutables = Seq("python.exe", "python3.exe") // Windows 可执行文件名

    // 4. 尝试在这些目录中查找 python.exe 或 python3.exe
    val commonPythonExecutables = Seq("python.exe", "python3.exe") // Windows 可执行文件名
//    if (sys.props("os.name").toLowerCase.contains("linux") || sys.props("os.name").toLowerCase.contains("mac")) {
//      // Unix-like 系统上的可执行文件名通常没有 .exe 后缀
//      // 这里的 .reverse.foreach 和 .filter(Files.exists) 保证了查找的健壮性
//      commonPythonExecutables.map(_.stripSuffix(".exe")) // 移除 .exe 后缀
//    }
    val executables = if (sys.props("os.name").toLowerCase.contains("linux") || sys.props("os.name").toLowerCase.contains("mac")) {
      // Unix-like 系统上的可执行文件名通常没有 .exe 后缀
      // 这里的 .reverse.foreach 和 .filter(Files.exists) 保证了查找的健壮性
      commonPythonExecutables.map(_.stripSuffix(".exe")) // 移除 .exe 后缀
    }else{
      commonPythonExecutables
    }


    for (dir <- pathDirs) {
      val dirPath = Paths.get(dir)
      if (Files.exists(dirPath)) { // 确保目录存在
        for (execName <- executables) {
          val fullPath = dirPath.resolve(execName)
          if (Files.exists(fullPath) && Files.isRegularFile(fullPath) && Files.isExecutable(fullPath)) {
//            println(s"Found Python executable at: ${fullPath.toString}")
            return fullPath.toString // 找到并返回第一个
          }
        }
      }
    }

    // 如果遍历所有 PATH 目录后仍未找到
    val errorMessage = "Failed to find 'python.exe' or 'python3.exe' in any PATH directory. " +
      "Please ensure Python is installed and its executable is in your system's PATH, " +
      "or the CONDA_PREFIX environment variable points to a valid Anaconda environment."
    println(s"Error: $errorMessage")
    sys.error(errorMessage) // 抛出错误终止程序
  }
}

class JavaUtilOnlyClassEnquirer extends ClassEnquirer {

  override def isJavaPackage(name: String): Boolean = name == "java.util" || name.startsWith("java.util.")

  override def getClassNames(pkgName: String): Array[String] = Array.empty

  override def getSubPackages(pkgName: String): Array[String] = Array.empty
}