package link.rdcn.dftree

import link.rdcn.dftree.FunctionWrapper.{JavaBin, JavaCode, PythonBin, PythonCode}
import link.rdcn.struct.{DataFrameStream, Row}
import link.rdcn.util.AutoClosingIterator
import link.rdcn.util.DataUtils.getDataFrameByStream
import org.json.{JSONArray, JSONObject}

import java.util.concurrent.Executors
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

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

  def execute(dataFrame: DataFrameStream): DataFrameStream
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

  override def execute(dataFrame: DataFrameStream): DataFrameStream = dataFrame
}

case class MapOp(functionWrapper: FunctionWrapper, input: Operation) extends Operation {

  override def operationType: String = "Map"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("function", functionWrapper.toJson)
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrameStream): DataFrameStream = {
    functionWrapper match {
      case JavaBin(serializedBase64) =>
        val in = input.execute(dataFrame)
        val stream = in.stream.map(functionWrapper.applyToInput(_, None)).map(_.asInstanceOf[Row])
        getDataFrameByStream(stream)
      case PythonCode(pythonCode, batchSize) =>
        val interp = JepInterpreterManager.getInterpreter
        val in = input.execute(dataFrame)
        val stream = in.stream.map(functionWrapper.applyToInput(_, Some(interp))).map(_.asInstanceOf[Row])
        getDataFrameByStream(stream)
    }
  }
}

case class FilterOp(functionWrapper: FunctionWrapper, input: Operation) extends Operation {

  override def operationType: String = "Filter"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("function", functionWrapper.toJson)
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrameStream): DataFrameStream = {
    functionWrapper match {
      case JavaBin(serializedBase64) =>
        val in = input.execute(dataFrame)
        val stream = in.stream.filter(functionWrapper.applyToInput(_, None).asInstanceOf[Boolean])
        DataFrameStream(in.schema, stream)
      case PythonCode(pythonCode, batchSize) =>
        val interp = JepInterpreterManager.getInterpreter
        val in = input.execute(dataFrame)
        val stream = in.stream.filter(functionWrapper.applyToInput(_, Some(interp)).asInstanceOf[Boolean])
        DataFrameStream(in.schema, stream)
    }
  }
}

case class LimitOp(limit: Int, input: Operation) extends Operation {

  override def operationType: String = "Limit"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("args", new JSONArray(Seq(limit).asJava))
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrameStream): DataFrameStream = {
    val in = input.execute(dataFrame)
    DataFrameStream(in.schema, in.stream.take(limit))
  }
}

case class SelectOp(input: Operation, columns: String*) extends Operation {

  override def operationType: String = "Select"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("args", new JSONArray(columns.asJava))
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrameStream): DataFrameStream = {
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
    DataFrameStream(selectedSchema, selectedStream)
  }
}

case class TransformerNode(functionWrapper: FunctionWrapper, input: Operation) extends Operation {

  private val singleThreadEc = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  override def operationType: String = "TransformerNode"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("function", functionWrapper.toJson)
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrameStream): DataFrameStream = {
    functionWrapper match {
      case JavaBin(serializedBase64) =>
        val in = input.execute(dataFrame)
        val stream = functionWrapper.applyToInput(in.stream, None).asInstanceOf[Iterator[Row]]
        getDataFrameByStream(stream)
      case JavaCode(javaCode, className) =>
        val in = input.execute(dataFrame)
        val stream = functionWrapper.applyToInput(in.stream, None).asInstanceOf[Iterator[Row]]
        getDataFrameByStream(stream)
      case PythonCode(pythonCode, batchSize) =>
        val jep = JepInterpreterManager.getInterpreter
        val in = input.execute(dataFrame)
        val stream: Iterator[Row] = functionWrapper.applyToInput(in.stream, Some(jep)).asInstanceOf[Iterator[Row]]
        getDataFrameByStream(stream)
      case PythonBin(functionID, functionName, whlPath, batchSize) =>
        Await.result(Future {
          val jep = JepInterpreterManager.getJepInterpreter(functionID, whlPath)
          val in = input.execute(dataFrame)
          val stream: AutoClosingIterator[Row] = AutoClosingIterator(
            functionWrapper.applyToInput(in.stream, Some(jep)).asInstanceOf[Iterator[Row]]
          )(jep.close())
          getDataFrameByStream(stream)
        }(singleThreadEc), Duration.Inf)
    }
  }
}