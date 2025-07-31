package link.rdcn.dftree

import akka.actor.ActorSystem
import jep.Jep
import link.rdcn.{ConfigLoader, SimpleSerializer}
import link.rdcn.client.GenericFunctionCall
import link.rdcn.struct.{DataFrame, LocalDataFrame, Row}
import link.rdcn.util.DataUtils.getDataFrameByStream
import link.rdcn.util.{AutoClosingIterator, ByteArrayClassLoader, DataUtils}
import org.json.JSONObject

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.net.{URL, URLClassLoader}
import java.nio.file.Paths
import java.util
import java.util.{Base64, ServiceLoader}
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter, seqAsJavaListConverter}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/2 15:40
 * @Modified By:
 */

sealed trait FunctionWrapper {
  def toJson: JSONObject

  def applyToInput(input: Any, interpOpt: Option[Jep] = None): Any
}

object FunctionWrapper {
  val operatorDir = Paths.get(getClass.getClassLoader.getResource("").toURI).toString
  implicit val system: ActorSystem = ActorSystem("SharedHttpClient")
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      system.terminate()
    }
  })
  val operatorClient = new OperatorClient("10.0.89.38",8088,system = system)

  case class PythonCode(code: String, batchSize: Int = 100) extends FunctionWrapper {
    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", LangType.PYTHON_CODE.name)
      jo.put("code", code)
    }

    override def toString(): String = "PythonCodeNode Function"

    override def applyToInput(input: Any, interpOpt: Option[Jep]): Any = {
      val interp = interpOpt.getOrElse(throw new IllegalArgumentException("Python interpreter is required"))
      input match {
        case row: Row =>
          val lst = new java.util.ArrayList[AnyRef]()
          row.toSeq.foreach(x => lst.add(x.asInstanceOf[AnyRef]))
          interp.set("input_data", lst)
          interp.exec(code)
          interp.getValue("output_data", classOf[Object])
        case (r1: Row, r2: Row) =>
          val lst1 = new java.util.ArrayList[AnyRef]()
          val lst2 = new util.ArrayList[AnyRef]()
          r1.toSeq.foreach(x => lst1.add(x.asInstanceOf[AnyRef]))
          r2.toSeq.foreach(x => lst2.add(x.asInstanceOf[AnyRef]))
          interp.set("input_data", (lst1, lst2))
          interp.exec(code)
          interp.getValue("output_data", classOf[Object])
        case iter: Iterator[Row] =>
          new Iterator[Row] {
            private val grouped: Iterator[Seq[Row]] = iter.grouped(batchSize)

            private var currentBatchIter: Iterator[Row] = Iterator.empty

            override def hasNext: Boolean = {
              while (!currentBatchIter.hasNext && grouped.hasNext) {
                val batch = grouped.next()

                // Convert Seq[Row] => java.util.List[java.util.List[AnyRef]]
                val javaBatch = new java.util.ArrayList[java.util.List[AnyRef]]()
                for (row <- batch) {
                  val rowList = new java.util.ArrayList[AnyRef]()
                  row.toSeq.foreach(v => rowList.add(v.asInstanceOf[AnyRef]))
                  javaBatch.add(rowList)
                }

                interp.set("input_data", javaBatch)
                interp.exec(code)
                val result = interp.getValue("output_data", classOf[java.util.List[java.util.List[AnyRef]]])
                val scalaRows = result.asScala.map(Row.fromJavaList)
                currentBatchIter = scalaRows.iterator
              }

              currentBatchIter.hasNext
            }

            override def next(): Row = {
              if (!hasNext) throw new NoSuchElementException("No more rows")
              currentBatchIter.next()
            }
          }
        case _ =>
          throw new IllegalArgumentException(s"Unsupported input: $input")
      }

    }
  }

  case class JavaBin(serializedBase64: String) extends FunctionWrapper {

    lazy val genericFunctionCall: GenericFunctionCall = {
      val restoredBytes = java.util.Base64.getDecoder.decode(serializedBase64)
      SimpleSerializer.deserialize(restoredBytes).asInstanceOf[GenericFunctionCall]
    }

    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", LangType.JAVA_BIN.name)
      jo.put("serializedBase64", serializedBase64)
    }

    override def toString(): String = "Java_bin Function"

    override def applyToInput(input: Any, interpOpt: Option[Jep] = None): Any = {
      input match {
        case row: Row => genericFunctionCall.transform(row)
        case (r1: Row, r2: Row) => genericFunctionCall.transform((r1, r2))
        case iter: Iterator[Row] => genericFunctionCall.transform(iter)
        case df: DataFrame => genericFunctionCall.transform(df)
        case other => throw new IllegalArgumentException(s"Unsupported input: $other")
      }
    }
  }

  case class JavaCode(clazzStr: String) extends FunctionWrapper {

    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", LangType.JAVA_CODE.name)
      jo.put("clazz", clazzStr)
    }

    override def applyToInput(input: Any, interpOpt: Option[Jep]): Any = {
      input match {
        case _: AutoClosingIterator[_] =>
          val clazzMap =  SimpleSerializer.deserialize(Base64.getDecoder.decode(clazzStr)).asInstanceOf[java.util.HashMap[String, Array[Byte]]]
          val classLoader = new ByteArrayClassLoader(clazzMap.asScala.toMap, Thread.currentThread().getContextClassLoader)
          val mainClassName = clazzMap.asScala.keys.find(!_.contains("$"))
      .getOrElse(throw new RuntimeException("cannot find main class name"))
          val clazz = classLoader.loadClass(mainClassName)
          val instance = clazz.getDeclaredConstructor().newInstance()
          val method = clazz.getMethod("transform", classOf[DataFrame])
          method.invoke(instance, getDataFrameByStream(input.asInstanceOf[AutoClosingIterator[Row]])).asInstanceOf[DataFrame]
        case other => throw new IllegalArgumentException(s"Unsupported input: $other")
      }

    }
  }

  case class PythonBin(functionID: String, functionName: String, whlPath: String, batchSize: Int = 100) extends FunctionWrapper {

    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", LangType.PYTHON_BIN.name)
      jo.put("functionID", functionID)
      jo.put("functionName", functionName)
      jo.put("whlPath", whlPath)
    }

    override def applyToInput(input: Any, interpOpt: Option[Jep]): Any = {
      val jep = interpOpt.getOrElse(throw new IllegalArgumentException("Python interpreter is required"))
      jep.eval("import link.rdcn.operators.registry as registry")
      jep.set("operator_name", functionName)
      jep.eval("func = registry.get_operator(operator_name)")
      input match {
        case rows: Iterator[Row] =>
          rows.grouped(batchSize).flatMap(rowSeq => {
            jep.set("input_rows", rowSeq.map(_.toSeq.asJava).asJava)
            jep.eval("output_rows = func(input_rows)")
            val result = jep.getValue("output_rows").asInstanceOf[java.util.List[java.util.List[Object]]]
            result.asScala.map(Row.fromJavaList(_))
          })
        case other => throw new IllegalArgumentException(s"Unsupported input: $other")
      }
    }
  }

  case class JavaJar(functionID: String) extends FunctionWrapper {
    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", LangType.JAVA_JAR.name)
      jo.put("functionID", functionID)
    }

    override def applyToInput(input: Any, interpOpt: Option[Jep] = None): Any = {
      val downloadFuture = operatorClient.downloadPackage(functionID, operatorDir)
      Await.result(downloadFuture, 30.seconds)
//      val jarPath = Paths.get(operatorDir, functionID + ".jar").toString()
      val jarPath = Paths.get("C:\\Users\\Yomi\\PycharmProjects\\Faird\\Faird\\faird-core\\target\\test-classes\\lib\\java\\"+ "faird-plugin-1.0-20250707.jar").toString()
      val jarFile = new java.io.File(jarPath)
      val urls = Array(jarFile.toURI.toURL)
      val parentLoader = getClass.getClassLoader
      val pluginLoader = new PluginClassLoader(urls, parentLoader)
      val serviceLoader = ServiceLoader.load(classOf[link.rdcn.client.dag.Transformer11], pluginLoader).iterator()
      if (!serviceLoader.hasNext) throw new Exception(s"No Transformer11 implementation class was found in this jar $jarPath")
      val udfFunction = serviceLoader.next()
      input match {
        case df: DataFrame => udfFunction.transform(df)
        case other => throw new IllegalArgumentException(s"Unsupported input: $other")
      }
    }
  }

  case class CppBin(functionID: String) extends FunctionWrapper {

    override def toJson: JSONObject = new JSONObject().put("type", LangType.CPP_BIN.name)
      .put("functionID", functionID)

    override def applyToInput(input: Any, interpOpt: Option[Jep]): Any = {
      val cppPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "cpp",functionID).toString()
      val pb = new ProcessBuilder(cppPath)
      val process = pb.start()
      val writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
      val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
      val inputDataFrame = input.asInstanceOf[DataFrame]
      val inputSchema = inputDataFrame.schema
      inputDataFrame.mapIterator[DataFrame](iter => {
        val stream = new Iterator[String] {
          override def hasNext: Boolean = iter.hasNext

          override def next(): String = {
            val row = iter.next()
            val jsonStr = row.toJsonString(inputSchema)
            // 写入 stdin
            writer.write(jsonStr)
            writer.newLine()
            writer.flush()
            // 从 stdout 读取一行响应 JSON
            val response = reader.readLine()
            if (response == null) throw new RuntimeException("Unexpected end of output from C++ process.")
            response
          }
        }
        val r = DataUtils.getStructTypeStreamFromJson(stream)
        val autoClosingIterator = AutoClosingIterator(r._1)(() => {
          iter.close()
          writer.close()
          reader.close()
          process.destroy()
        })
        LocalDataFrame(r._2, autoClosingIterator)
      })
    }
  }

  case class RepositoryOperator(functionID: String) extends FunctionWrapper {

    override def toJson: JSONObject = new JSONObject().put("type", LangType.REPOSITORY_OPERATOR.name)
      .put("functionID", functionID)

    override def applyToInput(input: Any, interpOpt: Option[Jep]): Any = {

    }
  }

  def apply(jsonObj: JSONObject): FunctionWrapper = {
    val funcType = jsonObj.getString("type")
    funcType match {
      case LangType.PYTHON_CODE.name => PythonCode(jsonObj.getString("code"))
      case LangType.JAVA_BIN.name => JavaBin(jsonObj.getString("serializedBase64"))
      case LangType.PYTHON_BIN.name => PythonBin(jsonObj.getString("functionID"), jsonObj.getString("functionName"), jsonObj.getString("whlPath"))
      case LangType.JAVA_CODE.name => JavaCode(jsonObj.getString("clazz"))
      case LangType.JAVA_JAR.name => JavaJar(jsonObj.getString("functionID"))
      case LangType.CPP_BIN.name => CppBin(jsonObj.getString("functionID"))
      case LangType.REPOSITORY_OPERATOR.name => RepositoryOperator(jsonObj.getString("functionID"))
    }
  }

  def getJavaSerialized(functionCall: GenericFunctionCall): JavaBin = {
    val objectBytes = SimpleSerializer.serialize(functionCall)
    val base64Str: String = java.util.Base64.getEncoder.encodeToString(objectBytes)
    JavaBin(base64Str)
  }

  private class PluginClassLoader(urls: Array[URL], parent: ClassLoader)
    extends URLClassLoader(urls, parent) {

    override def loadClass(name: String, resolve: Boolean): Class[_] = synchronized {
      // 必须由父加载器加载的类（避免 LinkageError）
      if (name.startsWith("scala.") ||
        name.startsWith("link.rdcn.") // 主程序中定义的接口、DataFrame等
      ) {
        return super.loadClass(name, resolve) // 委托给 parent
      }

      try {
        val clazz = findClass(name)
        if (resolve) resolveClass(clazz)
        clazz
      } catch {
        case _: ClassNotFoundException =>
          super.loadClass(name, resolve)
      }
    }
  }


}

