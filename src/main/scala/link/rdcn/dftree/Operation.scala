package link.rdcn.dftree

import link.rdcn.dftree.FunctionWrapper.{JavaBin, JavaCode, PythonBin, PythonCode}
import link.rdcn.struct.{DataFrame, Row}
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
    val jep = JepInterpreterManager.getInterpreter
    val in = input.execute(dataFrame)
    in.map(functionWrapper.applyToInput(_, Some(jep)).asInstanceOf[Row])
  }
}

case class FilterOp(functionWrapper: FunctionWrapper, input: Operation) extends Operation {

  override def operationType: String = "Filter"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("function", functionWrapper.toJson)
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrame): DataFrame = {
    val interp = JepInterpreterManager.getInterpreter
    val in = input.execute(dataFrame)
    in.filter(functionWrapper.applyToInput(_, Some(interp)).asInstanceOf[Boolean])
  }
}

case class LimitOp(n: Int, input: Operation) extends Operation {

  override def operationType: String = "Limit"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("args", new JSONArray(Seq(n).asJava))
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrame): DataFrame = {
    val in = input.execute(dataFrame)
    in.limit(n)
  }
}

case class SelectOp(input: Operation, columns: String*) extends Operation {

  override def operationType: String = "Select"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("args", new JSONArray(columns.asJava))
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrame): DataFrame = {
    val in = input.execute(dataFrame)
    in.select(columns: _*)
  }
}

case class TransformerNode(functionWrapper: FunctionWrapper, input: Operation) extends Operation {

  private val singleThreadEc = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  override def operationType: String = "TransformerNode"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("function", functionWrapper.toJson)
    .put("input", input.toJson)

  override def execute(dataFrame: DataFrame): DataFrame = {
    functionWrapper match {
      case PythonBin(functionID, functionName, whlPath, batchSize) =>
        Await.result(Future {
          val jep = JepInterpreterManager.getJepInterpreter(functionID, whlPath)
          val in = input.execute(dataFrame)
          in.mapIterator[DataFrame](iter => {
            val newStream =  functionWrapper.applyToInput(iter, Some(jep)).asInstanceOf[Iterator[Row]]
            getDataFrameByStream(AutoClosingIterator(newStream)(iter.onClose))
          })
        }(singleThreadEc), Duration.Inf)
      case _ =>
        val jep = JepInterpreterManager.getInterpreter
        val in = input.execute(dataFrame)
        in.mapIterator[DataFrame](iter => {
          val newStream = in.mapIterator[Iterator[Row]](functionWrapper.applyToInput(_, Some(jep)).asInstanceOf[Iterator[Row]])
          getDataFrameByStream(AutoClosingIterator(newStream)(iter.onClose))
        })

    }
  }
}