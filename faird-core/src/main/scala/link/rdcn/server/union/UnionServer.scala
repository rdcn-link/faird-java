package link.rdcn.server.union

import link.rdcn.ConfigLoader
import link.rdcn.client.{RemoteDataFrameProxy, UrlValidator}
import link.rdcn.client.dacp.DacpClient
import link.rdcn.client.dftp.DftpClient
import link.rdcn.optree.{Operation, RemoteSourceProxyOp, SourceOp, TransformerNode}
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics, DataProvider}
import link.rdcn.received.DataReceiver
import link.rdcn.server.dacp.DacpServer
import link.rdcn.server.dftp.{AllowAllAuthProvider, CookRequest, CookResponse, GetRequest, GetResponse}
import link.rdcn.struct.{DFRef, DataFrame, DataStreamSource, DefaultDataFrame, Row}
import link.rdcn.user.{AuthProvider, Credentials, DataOperationType, SignatureAuth}
import link.rdcn.util.{CodecUtils, KeyBasedAuthUtils}
import org.apache.jena.rdf.model.Model
import org.json.JSONObject

import java.util
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/20 13:57
 * @Modified By:
 */
class UnionServer(dataProvider: DataProvider, dataReceiver: DataReceiver, authProvider: AuthProvider) extends DacpServer(dataProvider, dataReceiver, authProvider) {

  private val endpoints = mutable.ListBuffer[Endpoint]()

  private val endpointClientsMap = mutable.Map[String, DftpClient]()
  private val endpointMap = mutable.Map[String, Endpoint]()

  val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
  val recipeCounter = new AtomicLong(0L)

  def addEndpoint(endpoints: Endpoint*): Unit = {
    endpoints.foreach(endpoint => {
      this.endpointClientsMap.put(endpoint.url, endpoint.getDftpClient(createSignature(-1)))
      this.endpoints.append(endpoint)
      this.endpointMap.put(endpoint.url, endpoint)
    })
  }

  override def doListDataSets(): DataFrame = {
    val dataSets = endpointClientsMap.toList.map(kv => kv._2.get(s"${kv._1}/listDataSets"))
    mergeDataFrame(dataSets)
  }

  override def doListDataFrames(listDataFrameUrl: String): DataFrame = {
    val dataFrames: mutable.Iterable[DataFrame] = endpointClientsMap.map(kv => (kv._2, kv._2.get(s"${kv._1}/listDataSets")))
      .map(kv => {
        mergeDataFrame(kv._2.mapIterator[Seq[DataFrame]](rows => rows.map(row => kv._1.get(row.getAs[DFRef](3).url)).toSeq).toList)
      })
    mergeDataFrame(dataFrames.toList)
  }

  override def doListHostInfo(): DataFrame = {
    val dataFrames = endpointClientsMap.toList.map(kv => kv._2.get(s"${kv._1}/listHostInfo"))
    mergeDataFrame(dataFrames)
  }

  override def doCook(request: CookRequest, response: CookResponse): Unit = {
    val operation = request.getOperation
    operation match {
      case s: SourceOp =>
        val baseUrlAndPath: (String, String) = UrlValidator.extractBaseUrlAndPath(s.dataFrameUrl) match {
          case Right((baseUrl, path)) => (baseUrl, path)
          case Left(message) => (this.baseUrl, s.dataFrameUrl)
        }
        if (baseUrlAndPath._1 == this.baseUrl) {
          baseUrlAndPath._2 match {
            case "/listDataSets" => response.sendDataFrame(doListDataSets())
            case "/listHostInfo" => response.sendDataFrame(doListHostInfo())
            case "/listDataFrames" => response.sendDataFrame(doListDataFrames(s.dataFrameUrl))
            case other => response.sendError(404, s"not found resource ${baseUrl}${other}")
          }
        } else {
          try {
            val client = endpointClientsMap.getOrElse(baseUrlAndPath._1, throw new Exception(s"Access to FaridServer ${s.dataFrameUrl} is denied"))
            response.sendDataFrame(client.getByOperation(operation))
          } catch {
            case e: Exception =>
              logger.error(e)
              response.sendError(500, e.getMessage)
          }
        }
      case other: Operation => response.sendDataFrame(createRecipe(other).execute())
    }
  }

  private def createSignature(expirationTime: Long): SignatureAuth = {
    val privateKey = ConfigLoader.fairdConfig.privateKey
    if (privateKey.isEmpty) throw new Exception("Private key not found. Please configure private key information for this UnionServer.")
    else {
      val nonce = UUID.randomUUID().toString
      val issueTime = System.currentTimeMillis()
      val jo = new JSONObject().put("serverId", this.baseUrl)
        .put("nonce", nonce)
        .put("issueTime", issueTime)
        .put("validTo", issueTime + expirationTime)

      val signature = KeyBasedAuthUtils.signData(privateKey.get, CodecUtils.encodeString(jo.toString))
      SignatureAuth(this.baseUrl, nonce, issueTime, issueTime + expirationTime, signature)
    }
  }

  private def getOperationSource(operations: Seq[Operation]): Seq[Operation] = {
    operations.map(operation => {
      operation match {
        case s: SourceOp => Seq(s)
        case other: Operation => getOperationSource(other.inputs)
      }
    }).flatten
  }

  private def rebuildOperation(operation: Operation, baseUrl: String): Operation = {
    val inputs = operation.inputs.map(operation => operation match {
      case s: SourceOp =>
        if (s.dataFrameUrl.startsWith(baseUrl)) s else {
          val certificate = createSignature(60L * 60 * 1000) //default expirationTime 1h
          RemoteSourceProxyOp(s.dataFrameUrl, certificate.toJson().toString)
        }
      case other: Operation => rebuildOperation(other, baseUrl)
    })
    operation.setInputs(inputs: _*)
  }

  private def getRecipeId(): String = {
    val timestamp = ZonedDateTime.now(ZoneId.of("UTC")).format(formatter)
    val seq = recipeCounter.incrementAndGet()
    s"job-${timestamp}-${seq}"
  }

  private case class Recipe(
                             jobId: String,
                             operation: Operation,
                             executionNodeBaseUrl: String,
                             attributes: Map[String, String] = Map()
                           ) {
    def execute(): DataFrame = {
      val client = endpointClientsMap.getOrElse(executionNodeBaseUrl, throw new Exception(s"Access to FaridServer ${executionNodeBaseUrl} is denied"))
      client.getByOperation(operation)
    }

    //TODO kill Job
    def close(): Unit = {}
  }

  //  TODO 根据计算资源判定执行节点
  //可强制指定执行节点
  private def createRecipe(operation: Operation, executionNodeBaseUrl: Option[String] = None): Recipe = {
    if (executionNodeBaseUrl.isEmpty) {
      val baseUrl = getOperationSource(Seq(operation)).map(_.asInstanceOf[SourceOp].dataFrameUrl)
        .map(UrlValidator.extractBase(_).get._1)
        .groupBy(identity)
        .mapValues(_.size)
        .maxBy(_._2)._1
      val reOperation = rebuildOperation(operation, baseUrl)
      Recipe(getRecipeId(), reOperation, baseUrl)
    } else {
      Recipe(getRecipeId(), rebuildOperation(operation, executionNodeBaseUrl.get), executionNodeBaseUrl.get)
    }
  }

  private def mergeDataFrame(dataFrames: List[DataFrame]): DataFrame = {
    val stream: Iterator[Row] = dataFrames.map(df => df.mapIterator[Iterator[Row]](iter => iter)).reduce(_ ++ _)
    val schema = dataFrames.head.schema
    DefaultDataFrame(schema, stream)
  }
}

object UnionServer {

  def apply(endpoints: List[Endpoint]): UnionServer = {
    val unionServer = new UnionServer(dataProvider, dataReceiver, authProvider)
    unionServer.addEndpoint(endpoints: _*)
    unionServer
  }

  def connect(urls: String*): UnionServer = {
    val endpoints = urls.map(url => {
      UrlValidator.validate(url) match {
        case Right(value) => Endpoint(value._2, value._3.getOrElse(3101))
        case Left(message) => throw new IllegalArgumentException(message)
      }
    }).toList
    UnionServer(endpoints)
  }

  private val authProvider = new AllowAllAuthProvider

  private val dataProvider = new DataProvider {
    override def listDataSetNames(): util.List[String] = ???

    override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = ???

    override def listDataFrameNames(dataSetId: String): util.List[String] = ???

    override def getDataStreamSource(dataFrameName: String): DataStreamSource = ???

    override def getDocument(dataFrameName: String): DataFrameDocument = ???

    override def getStatistics(dataFrameName: String): DataFrameStatistics = ???
  }
  private val dataReceiver = new DataReceiver {
    override def start(): Unit = ???

    override def receiveRow(dataFrame: DataFrame): Unit = ???

    override def finish(): Unit = ???
  }
}

case class Endpoint(
                     host: String,
                     port: Int
                   ) {
  // 解析Url获取的key
  def url: String = s"${DacpClient.protocolSchema}://${host}:${port}"

  def getDftpClient(credentials: Credentials): DftpClient = DacpClient.connect(url, credentials)

}


