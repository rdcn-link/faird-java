package link.rdcn.dftree

import jep.SharedInterpreter
import link.rdcn.SimpleSerializer
import link.rdcn.client.GenericFunctionCall
import link.rdcn.struct.Row
import org.json.JSONObject

import java.util
import scala.collection.JavaConverters.asScalaBufferConverter


/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/2 15:40
 * @Modified By:
 */

sealed trait FunctionWrapper {
  def toJson: JSONObject
  def applyToInput(input: Any, interpOpt: Option[SharedInterpreter] = None): Any
}

object FunctionWrapper {

  case class JsonCode(code: String, batchSize: Int = 100) extends FunctionWrapper {
    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", LangType.PYTHON_CODE.name)
      jo.put("code", code)
    }
    override def toString(): String = "JsonCode Function"

    override def applyToInput(input: Any, interpOpt: Option[SharedInterpreter]): Any = {
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
    override def applyToInput(input: Any, interpOpt: Option[SharedInterpreter] = None): Any = {
      input match {
        case row: Row               => genericFunctionCall.transform(row)
        case (r1: Row, r2: Row)     => genericFunctionCall.transform((r1, r2))
        case iter: Iterator[Row]    => genericFunctionCall.transform(iter)
        case other                  => throw new IllegalArgumentException(s"Unsupported input: $other")
      }
    }
  }

  def apply(jsonObj: JSONObject): FunctionWrapper = {
    val funcType = jsonObj.getString("type")
    funcType match {
      case LangType.PYTHON_CODE.name => JsonCode(jsonObj.getString("code"))
      case LangType.JAVA_BIN.name => JavaBin(jsonObj.getString("serializedBase64"))
    }
  }

  def getJavaSerialized(functionCall: GenericFunctionCall): JavaBin = {
    val objectBytes = SimpleSerializer.serialize(functionCall)
    val base64Str: String = java.util.Base64.getEncoder.encodeToString(objectBytes)
    JavaBin(base64Str)
  }
}

