package link.rdcn.optree

import link.rdcn.client.UrlValidator
import link.rdcn.client.dacp.DacpClient
import link.rdcn.optree.FunctionWrapper._
import link.rdcn.struct.{DataFrame, Row}
import link.rdcn.user.TokenAuth
import link.rdcn.util.ClosableIterator
import link.rdcn.util.DataUtils.getDataFrameByStream
import org.json.{JSONArray, JSONObject}

import java.nio.file.Paths
import java.util.concurrent.Executors
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/1 17:02
 * @Modified By:
 */
trait ExecutionContext {
  def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame]
}

sealed trait Operation {

  var inputs: Seq[Operation]

  def setInputs(operations: Operation*): Operation = {
    inputs = operations
    this
  }
  def operationType: String

  def toJson: JSONObject

  def toJsonString: String = toJson.toString

  def execute(ctx: ExecutionContext): DataFrame
}

object Operation {
  def fromJsonString(json: String, sourceList: ListBuffer[String]): Operation = {
    val parsed: JSONObject = new JSONObject(json)
    val opType = parsed.getString("type")
   if (opType == "SourceOp") {
      sourceList.append(parsed.getString("dataFrameName"))
      SourceOp(parsed.getString("dataFrameName"))
    }else if(opType == "RemoteSourceProxyOp") {
     RemoteSourceProxyOp(parsed.getString("baseUrl")+parsed.getString("path"), parsed.getString("token"))
   }
    else {
      val ja: JSONArray = parsed.getJSONArray("input")
      val inputs = (0 until ja.length).map(ja.getJSONObject(_).toString()).map(fromJsonString(_, sourceList))
      opType match {
        case "Map" => MapOp(FunctionWrapper(parsed.getJSONObject("function")), inputs: _*)
        case "Filter" => FilterOp(FunctionWrapper(parsed.getJSONObject("function")), inputs: _*)
        case "Limit" => LimitOp(parsed.getJSONArray("args").getInt(0), inputs: _*)
        case "Select" => SelectOp(inputs.head, parsed.getJSONArray("args").toList.asScala.map(_.toString): _*)
        case "TransformerNode" => TransformerNode(FunctionWrapper(parsed.getJSONObject("function")), inputs: _*)
      }
    }
  }
}

case class SourceOp(dataFrameUrl: String) extends Operation {

  override var inputs: Seq[Operation] = Seq.empty

  override def operationType: String = "SourceOp"

  override def toJson: JSONObject = new JSONObject().put("type", operationType).put("dataFrameName", dataFrameUrl)

  override def execute(ctx: ExecutionContext): DataFrame = ctx.loadSourceDataFrame(dataFrameUrl)
    .getOrElse(throw new Exception(s"dataFrame $dataFrameUrl not found"))
}

case class RemoteSourceProxyOp(url: String, certificate: String) extends Operation {

  val baseUrlAndPath = UrlValidator.extractBaseUrlAndPath(url)
  var baseUrl: String = _
  var path: String = _
  baseUrlAndPath match {
    case Right(value) =>
      baseUrl = value._1
      path = value._2
    case Left(message) => throw new IllegalArgumentException(message)
  }

  override var inputs: Seq[Operation] = Seq.empty

  override def operationType: String = "RemoteSourceProxyOp"

  override def toJson: JSONObject = new JSONObject().put("type", operationType)
    .put("baseUrl", baseUrl).put("path", path).put("token", certificate)

  override def execute(ctx: ExecutionContext): DataFrame = {
    val dftpClient = DacpClient.connect(baseUrl, TokenAuth(certificate))
    dftpClient.get(baseUrl + path)
  }
}

case class MapOp(functionWrapper: FunctionWrapper, inputOperations: Operation*) extends Operation {

  override var inputs = inputOperations

  override def operationType: String = "Map"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(op => ja.put(op.toJson))
    new JSONObject().put("type", operationType)
      .put("function", functionWrapper.toJson)
      .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    val jep = JepInterpreterManager.getInterpreter
    val in = inputs.head.execute(ctx)
    in.map(functionWrapper.applyToInput(_, Some(jep)).asInstanceOf[Row])
  }
}

case class FilterOp(functionWrapper: FunctionWrapper, inputOperations: Operation*) extends Operation {

  override var inputs = inputOperations

  override def operationType: String = "Filter"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(op => ja.put(op.toJson))
    new JSONObject().put("type", operationType)
    .put("function", functionWrapper.toJson)
    .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    val interp = JepInterpreterManager.getInterpreter
    val in = inputs.head.execute(ctx)
    in.filter(functionWrapper.applyToInput(_, Some(interp)).asInstanceOf[Boolean])
  }
}

case class LimitOp(n: Int, inputOperations: Operation*) extends Operation {

  override var inputs = inputOperations

  override def operationType: String = "Limit"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(op => ja.put(op.toJson))
    new JSONObject().put("type", operationType)
      .put("args", new JSONArray(Seq(n).asJava))
      .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    val in = inputs.head.execute(ctx)
    in.limit(n)
  }
}

case class SelectOp(input: Operation, columns: String*) extends Operation {

  override var inputs: Seq[Operation] = Seq(input)

  override def operationType: String = "Select"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(op => ja.put(op.toJson))
    new JSONObject().put("type", operationType)
    .put("args", new JSONArray(columns.asJava))
    .put("input", ja)
  }

  override def execute(ctx: ExecutionContext): DataFrame = {
    val in = input.execute(ctx)
    in.select(columns: _*)
  }
}

case class TransformerNode(functionWrapper: FunctionWrapper, inputOperations: Operation*) extends Operation {

  private val singleThreadEc = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  override var inputs: Seq[Operation] = inputOperations

  override def operationType: String = "TransformerNode"

  override def toJson: JSONObject = {
    val ja = new JSONArray()
    inputs.foreach(in => ja.put(in.toJson))
    new JSONObject().put("type", operationType)
      .put("function", functionWrapper.toJson)
      .put("input", ja)
  }
  override def execute(ctx: ExecutionContext): DataFrame = {
    functionWrapper match {
      case PythonBin(functionID, functionName, whlPath, batchSize) =>
        Await.result(Future {
          val jep = JepInterpreterManager.getJepInterpreter(functionID, whlPath)
          val in = inputs.head.execute(ctx)
          in.mapIterator[DataFrame](iter => {
            val newStream =  functionWrapper.applyToInput(iter, Some(jep)).asInstanceOf[Iterator[Row]]
            getDataFrameByStream(ClosableIterator(newStream)(iter.onClose))
          })
        }(singleThreadEc), Duration.Inf)
      case j: JavaBin =>
        inputs.length match {
          case 1 => j.applyToInput(inputs.head.execute(ctx)).asInstanceOf[DataFrame]
          case 2 => j.applyToInput((inputs.head.execute(ctx), inputs.last.execute(ctx))).asInstanceOf[DataFrame]
        }
      case javaJar: JavaJar => {
        val in = inputs.head.execute(ctx)
        javaJar.applyToInput(in).asInstanceOf[DataFrame]
      }
      case repositoryOperator: RepositoryOperator =>
        val in = inputs.head.execute(ctx)
        val id = repositoryOperator.functionID
        val downloadFuture = operatorClient.downloadPackage(repositoryOperator.functionID, operatorDir)
        Await.result(downloadFuture, 30.seconds)
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
              TransformerNode(FunctionWrapper(jo).asInstanceOf[PythonBin], inputs: _*).execute(ctx)
            }(singleThreadEc), Duration.Inf)
          case _ => throw new IllegalArgumentException(s"Unsupported operator type: ${info.get("type")}")
        }
      case _ =>
        val jep = JepInterpreterManager.getInterpreter
        val in = inputs.head.execute(ctx)
        in.mapIterator[DataFrame](iter => {
          functionWrapper.applyToInput(iter, Some(jep)).asInstanceOf[DataFrame]
        })
    }
  }
}