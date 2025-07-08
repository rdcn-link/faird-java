package link.rdcn.dftree

import jep.SharedInterpreter
import link.rdcn.SimpleSerializer
import link.rdcn.client.DFOperation
import link.rdcn.dftree.FunctionWrapper.{JavaSerialized, JsonCode}
import link.rdcn.struct.{DataFrame, Row}
import org.json.JSONObject
/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/1 17:02
 * @Modified By:
 */

sealed trait OperationNode {
  def toJson: JSONObject
  def toJsonString: String = toJson.toString
  def execute(dataFrame: DataFrame): Iterator[Row]
}

object OperationNode {
  def fromJsonString(json: String): OperationNode = {
    val parsed: JSONObject = new JSONObject(json)
    OperationType.fromName(parsed.getString("type")) match {
      case OperationType.Source => SourceNode()
      case _ =>
        val operationType: OperationType = OperationType.fromName(parsed.getString("type"))
        val lang: LangType = LangType.fromName(parsed.getString("lang"))
        val functionWrapper: FunctionWrapper = FunctionWrapper(parsed.getJSONObject("function"))
        val input: OperationNode = fromJsonString(parsed.getJSONObject("input").toString)
        TransformNode(operationType, lang, functionWrapper, input)
    }
  }
}

case class SourceNode() extends OperationNode {

  override def toJson: JSONObject = new JSONObject().put("type", "Source")

  override def execute(dataFrame: DataFrame): Iterator[Row] = dataFrame.stream
}

case class TransformNode(operationType: OperationType, lang: LangType, functionWrapper: FunctionWrapper, input: OperationNode) extends OperationNode {
  private val python_batch_size = 500
  override def toJson: JSONObject = new JSONObject().put("type", operationType.name)
    .put("lang", lang.name).put("function", functionWrapper.toJson)
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrame): Iterator[Row] = {
    functionWrapper match {
      case JsonCode(pythonCode) =>
        val in = input.execute(dataFrame: DataFrame)
        in.grouped(python_batch_size).flatMap(rows => {
          val javaInput = new java.util.ArrayList[java.util.List[AnyRef]]()
          rows.foreach { row =>
            val javaRow = new java.util.ArrayList[AnyRef]()
            row.toSeq.foreach(x => javaRow.add(x.asInstanceOf[AnyRef]))
            javaInput.add(javaRow)
          }
          val interp = new SharedInterpreter()
          try {
            interp.set("input_data", javaInput)
            interp.exec(pythonCode)
            val pyResult = interp.getValue("output_data", classOf[java.util.List[_]])

            val scalaResult: List[List[Any]] = pyResult.asInstanceOf[java.util.List[java.util.List[_]]]
              .toArray
              .map(_.asInstanceOf[java.util.List[_]].toArray.toList)
              .toList
            scalaResult.map(Row.fromSeq(_))
          } finally {
            interp.close()
          }
        })
      case JavaSerialized(serializedBase64) =>
        val restoredBytes: Array[Byte] = java.util.Base64.getDecoder.decode(serializedBase64)
        val dfOperation = SimpleSerializer.deserialize(restoredBytes).asInstanceOf[DFOperation]
        val in = input.execute(dataFrame: DataFrame)
        dfOperation.transform(in)
    }
  }
}