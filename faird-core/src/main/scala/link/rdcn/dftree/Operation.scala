package link.rdcn.dftree

import link.rdcn.client.DataFrameCall
import link.rdcn.dftree.FunctionWrapper._
import link.rdcn.struct.{DataFrame, Row}
import link.rdcn.util.ClosableIterator
import link.rdcn.util.DataUtils.getDataFrameByStream
import org.json.{JSONArray, JSONObject}

import java.nio.file.Paths
import java.util.concurrent.Executors
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import scala.concurrent.duration.{Duration, DurationInt}
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
            getDataFrameByStream(ClosableIterator(newStream)(iter.onClose))
          })
        }(singleThreadEc), Duration.Inf)
      case j: JavaBin if j.genericFunctionCall.isInstanceOf[DataFrameCall] => {
        val in = input.execute(dataFrame)
        j.genericFunctionCall.asInstanceOf[DataFrameCall].transform(in).asInstanceOf[DataFrame]
      }
      case javaJar: JavaJar => {
        val in = input.execute(dataFrame)
        javaJar.applyToInput(in).asInstanceOf[DataFrame]
      }
      case repositoryOperator: RepositoryOperator =>
        val in = input.execute(dataFrame)
        val id = repositoryOperator.functionID
        val infoFuture = operatorClient.getOperatorInfo(id)
        val info = Await.result(infoFuture, 30.seconds)
        val fileName = info.get("fileName").toString
        info.get("type") match {
          case LangType.CPP_BIN.name =>
            CppBin(id).applyToInput(in).asInstanceOf[DataFrame]
          case LangType.JAVA_JAR.name =>
            JavaJar(id,fileName).applyToInput(in).asInstanceOf[DataFrame]
          case LangType.PYTHON_BIN.name =>
            val downloadFuture = operatorClient.downloadPackage(id, operatorDir)
            Await.result(downloadFuture, 30.seconds)
            val whlPath = Paths.get(operatorDir, fileName).toString()
            val jo = new JSONObject()
            Await.result(Future {
            jo.put("type", LangType.PYTHON_BIN.name)
            jo.put("functionID", id)
            jo.put("functionName", info.get("functionName"))
            jo.put("whlPath", whlPath)
              TransformerNode(FunctionWrapper(jo).asInstanceOf[PythonBin], input).execute(dataFrame)
            }(singleThreadEc), Duration.Inf)
          case _ => throw new IllegalArgumentException(s"Unsupported operator type: ${info.get("type")}")
        }
      case _ =>
        val jep = JepInterpreterManager.getInterpreter
        val in = input.execute(dataFrame)
        in.mapIterator[DataFrame](iter => {
          functionWrapper.applyToInput(iter, Some(jep)).asInstanceOf[DataFrame]
        })
    }
  }
}