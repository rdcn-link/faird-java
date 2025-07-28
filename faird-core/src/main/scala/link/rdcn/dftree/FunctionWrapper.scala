package link.rdcn.dftree

import jep.Jep
import link.rdcn.SimpleSerializer
import link.rdcn.client.GenericFunctionCall
import link.rdcn.client.dag.Transformer11
import link.rdcn.struct.{DataFrame, Row}
import link.rdcn.util.AutoClosingIterator
import link.rdcn.util.DataUtils.getDataFrameByStream
import org.json.JSONObject
import org.codehaus.janino.SimpleCompiler

import java.net.URLClassLoader
import java.util
import java.util.ServiceLoader
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}


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
          interp.set("input_data",  (lst1, lst2))
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
        case row: Row               => genericFunctionCall.transform(row)
        case (r1: Row, r2: Row)     => genericFunctionCall.transform((r1, r2))
        case iter: Iterator[Row]    => genericFunctionCall.transform(iter)
        case df: DataFrame          => genericFunctionCall.transform(df)
        case other                  => throw new IllegalArgumentException(s"Unsupported input: $other")
      }
    }
  }

  case class JavaCode(javaCode: String, className: String) extends FunctionWrapper {

    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", LangType.JAVA_CODE.name)
      jo.put("javaCode", javaCode)
      jo.put("className", className)
    }

    override def applyToInput(input: Any, interpOpt: Option[Jep]): Any = {
      input match {
        case _: AutoClosingIterator[_] => val
          compiler = new SimpleCompiler()
          compiler.cook(javaCode)
          val clazz = compiler.getClassLoader.loadClass(className)
          val instance = clazz.getDeclaredConstructor().newInstance()
          val method = clazz.getMethod("transform", classOf[DataFrame])
          method.invoke(instance, getDataFrameByStream(input.asInstanceOf[AutoClosingIterator[Row]])).asInstanceOf[DataFrame]
        case other =>  throw new IllegalArgumentException(s"Unsupported input: $other")
      }

    }
  }

  case class PythonBin(functionID: String, functionName: String, whlPath: String, batchSize: Int = 100) extends FunctionWrapper {

    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", LangType.PYTHON_BIN.name)
      jo.put("functionId", functionID)
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

  case class JavaJar(functionID: String, jarPath: String) extends FunctionWrapper {
    //对接算子库后去掉 jarPath
    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", LangType.JAVA_JAR.name)
      jo.put("functionId", functionID)
      jo.put("jarPath", jarPath)
    }

    override def applyToInput(input: Any, interpOpt: Option[Jep] = None): Any = {
      val jarFile = new java.io.File(jarPath)
      val urls = Array(jarFile.toURI.toURL)
      val classLoader = new URLClassLoader(urls, null) //null 表示以 bootstrap classloader 为父类加载器，也就是完全隔离宿主环境
      val serviceLoader = ServiceLoader.load(classOf[Transformer11], classLoader).iterator()
      if(!serviceLoader.hasNext) throw new Exception(s"No Transformer11 implementation class was found in this jar $jarPath")
      val udfFunction = serviceLoader.next()
      input match {
        case df: DataFrame => udfFunction.transform(df)
        case other => throw new IllegalArgumentException(s"Unsupported input: $other")
      }
    }
  }

  def apply(jsonObj: JSONObject): FunctionWrapper = {
    val funcType = jsonObj.getString("type")
    funcType match {
      case LangType.PYTHON_CODE.name => PythonCode(jsonObj.getString("code"))
      case LangType.JAVA_BIN.name => JavaBin(jsonObj.getString("serializedBase64"))
      case LangType.PYTHON_BIN.name => PythonBin(jsonObj.getString("functionId"), jsonObj.getString("functionName"), jsonObj.getString("whlPath"))
      case LangType.JAVA_CODE.name => JavaCode(jsonObj.getString("javaCode"), jsonObj.getString("className"))
      case LangType.JAVA_JAR.name => JavaJar(jsonObj.getString("functionId"), jsonObj.getString("jarPath"))
    }
  }

  def getJavaSerialized(functionCall: GenericFunctionCall): JavaBin = {
    val objectBytes = SimpleSerializer.serialize(functionCall)
    val base64Str: String = java.util.Base64.getEncoder.encodeToString(objectBytes)
    JavaBin(base64Str)
  }
}

