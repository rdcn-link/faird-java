package link.rdcn.dftree

import jep.{ClassEnquirer, JepConfig, JepException, SharedInterpreter, SubInterpreter}
import link.rdcn.{ConfigLoader, Logging}

import java.nio.file.{Path, Paths}
import sys.process._
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
      val env = "PATH" -> sys.env.getOrElse("PATH", "")
      val cmd = Seq("python3", "-m", "pip", "install", "--upgrade", "--target", sitePackagePath, whlPath)
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
}

class JavaUtilOnlyClassEnquirer extends ClassEnquirer {

  override def isJavaPackage(name: String): Boolean = name == "java.util" || name.startsWith("java.util.")

  override def getClassNames(pkgName: String): Array[String] = Array.empty

  override def getSubPackages(pkgName: String): Array[String] = Array.empty
}