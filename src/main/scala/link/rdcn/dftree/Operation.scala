package link.rdcn.dftree

import jep.SharedInterpreter
import link.rdcn.dftree.FunctionWrapper.{JavaBin, JsonCode}
import link.rdcn.struct.{DataFrame, Row}
import link.rdcn.util.DataUtils.getDataFrameByStream
import org.json.{JSONArray, JSONObject}

import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/1 17:02
 * @Modified By:
 */

sealed trait Operation {
  def operationType: String

  def toJson: JSONObject

  def toJsonString: String = toJson.toString

  def execute(dataFrame: DataFrame): DataFrame
}

object Operation {
  def fromJsonString(json: String): Operation = {
    val parsed: JSONObject = new JSONObject(json)
    val opType = parsed.getString("type")
    if (opType == "SourceOp") SourceOp()
    else {
      val input: Operation = fromJsonString(parsed.getJSONObject("input").toString)
      opType match {
        case "TransformerNode" => TransformerNode(FunctionWrapper(parsed.getJSONObject("function")), input)
        case "Map" => MapOp(FunctionWrapper(parsed.getJSONObject("function")), input)
        case "Filter" => FilterOp(FunctionWrapper(parsed.getJSONObject("function")), input)
        case "Limit" => LimitOp(parsed.getJSONArray("args").getInt(0), input)
        case "Select" => SelectOp(input, parsed.getJSONArray("args").toList.asScala.map(_.toString): _*)
      }
    }
  }
}

case class SourceOp() extends Operation {

  override def operationType: String = "SourceOp"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)

  override def execute(dataFrame: DataFrame): DataFrame = dataFrame
}

case class MapOp(functionWrapper: FunctionWrapper, input: Operation) extends Operation {

  override def operationType: String = "Map"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("function", functionWrapper.toJson)
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrame): DataFrame = {
    functionWrapper match {
      case JavaBin(serializedBase64) =>
        val in = input.execute(dataFrame)
        val stream = in.stream.map(functionWrapper.applyToInput(_, None)).map(_.asInstanceOf[Row])
        getDataFrameByStream(stream)
      case JsonCode(pythonCode, batchSize) =>
        val interp = new SharedInterpreter()
        try {
          val in = input.execute(dataFrame)
          val stream = in.stream.map(functionWrapper.applyToInput(_, Some(interp))).map(_.asInstanceOf[Row])
          getDataFrameByStream(stream)
        } finally {
          interp.close()
        }
    }
  }
}

case class FilterOp(functionWrapper: FunctionWrapper, input: Operation) extends Operation {

  override def operationType: String = "Filter"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("function", functionWrapper.toJson)
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrame): DataFrame = {
    functionWrapper match {
      case JavaBin(serializedBase64) =>
        val in = input.execute(dataFrame)
        val stream = in.stream.filter(functionWrapper.applyToInput(_, None).asInstanceOf[Boolean])
        DataFrame(in.schema, stream)
      case JsonCode(pythonCode, batchSize) =>
        val interp = new SharedInterpreter()
        try {
          val in = input.execute(dataFrame)
          val stream = in.stream.filter(functionWrapper.applyToInput(_, Some(interp)).asInstanceOf[Boolean])
          DataFrame(in.schema, stream)
        } finally {
          interp.close()
        }
    }
  }
}

case class LimitOp(limit: Int, input: Operation) extends Operation {

  override def operationType: String = "Limit"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("args", new JSONArray(Seq(limit).asJava))
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrame): DataFrame = {
    val in = input.execute(dataFrame)
    DataFrame(in.schema, in.stream.take(limit))
  }
}

case class SelectOp(input: Operation, columns: String*) extends Operation {

  override def operationType: String = "Select"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("args", new JSONArray(columns.asJava))
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrame): DataFrame = {
    val in = input.execute(dataFrame)
    val selectedSchema = in.schema.select(columns: _*)
    val selectedStream = in.stream.map { row =>
      val selectedValues = columns.map { colName =>
        val idx = in.schema.indexOf(colName).getOrElse {
          throw new IllegalArgumentException(s"列名 '$colName' 不存在")
        }
        row.get(idx)
      }
      Row.fromSeq(selectedValues)
    }
    DataFrame(selectedSchema, selectedStream)
  }
}

case class TransformerNode(functionWrapper: FunctionWrapper, input: Operation) extends Operation {

  override def operationType: String = "TransformerNode"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("function", functionWrapper.toJson)
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrame): DataFrame = {
    functionWrapper match {
      case JavaBin(serializedBase64) =>
        val in = input.execute(dataFrame)
        val stream = functionWrapper.applyToInput(in.stream, None).asInstanceOf[Iterator[Row]]
        getDataFrameByStream(stream)
      case JsonCode(pythonCode, batchSize) =>
        val interp = new SharedInterpreter()
        try {
          val in = input.execute(dataFrame)
          val stream = functionWrapper.applyToInput(in.stream, Some(interp)).asInstanceOf[Iterator[Row]]
          getDataFrameByStream(stream)
        } finally {
          interp.close()
        }
    }
  }
}