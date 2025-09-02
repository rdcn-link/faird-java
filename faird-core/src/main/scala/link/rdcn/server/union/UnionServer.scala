package link.rdcn.server.union

import link.rdcn.client.UrlValidator
import link.rdcn.client.dacp.FairdClient
import link.rdcn.client.dftp.DftpClient
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics, DataProvider}
import link.rdcn.received.DataReceiver
import link.rdcn.server.dacp.DacpServer
import link.rdcn.server.dftp.{GetRequest, GetResponse}
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame, Row}
import link.rdcn.user.{AuthProvider, Credentials}
import org.apache.jena.rdf.model.Model

import java.util
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/20 13:57
 * @Modified By:
 */
class UnionServer private(dataProvider: DataProvider, dataReceiver: DataReceiver, authProvider: AuthProvider) extends DacpServer(dataProvider, dataReceiver, authProvider)  {

  private val endpoints = mutable.ListBuffer[Endpoint]()

  private val endpointClientsMap = mutable.Map[String, DftpClient]()

  def addEndpoint(endpoints: Endpoint*): Unit = {
    endpoints.foreach(endpoint => {
      this.endpointClientsMap.put(endpoint.url, endpoint.getDftpClient())
      this.endpoints.append(endpoint)
    })
  }

  override def doListDataSets(): DataFrame = {
    val dataSets = endpointClientsMap.toList.map(kv => kv._2.get(s"${kv._1}/listDataSets"))
    mergeDataFrame(dataSets)
  }

  override def doListDataFrames(listDataFrameUrl: String): DataFrame = {
    val hostUrl = UrlValidator.extractBase(listDataFrameUrl).get
    val dftpClient = endpointClientsMap.get(hostUrl._1).get
    dftpClient.get(listDataFrameUrl)
  }

  override def doListHostInfo(): DataFrame = {
    val dataFrames = endpointClientsMap.toList.map(kv => kv._2.get(s"${kv._1}/listHostInfo"))
    mergeDataFrame(dataFrames)
  }

  override def doGet(request: GetRequest, response: GetResponse): Unit = {
    val requestBaseUrl = request.getRequestedBaseUrl
    requestBaseUrl match {
      case unionBaseUrl if unionBaseUrl == this.baseUrl || unionBaseUrl == "" =>
        val path = request.getRequestedPath()
        if(Seq("/listDataSets","/listDataFrames", "/listHostInfo").contains(path)) super.doGet(request, response)
        else response.sendError(404, s"not found resource ${baseUrl}${request.getRequestedBaseUrl()}")
      case endpointUrl => {
        val client = endpointClientsMap.get(endpointUrl)
        if(client.isEmpty) response.sendError(400, s"bad request url $endpointUrl")
        else
          response.sendDataFrame(client.get.get(s"$endpointUrl${request.getRequestedPath}"))
      }
    }
    super.doGet(request, response)
  }

  private def mergeDataFrame(dataFrames: List[DataFrame]): DataFrame = {
    val stream: Iterator[Row]= dataFrames.map(df => df.mapIterator[Iterator[Row]](iter => iter)).reduce(_ ++ _)
    val schema = dataFrames.head.schema
    DefaultDataFrame(schema, stream)
  }
}

object UnionServer{

  def apply(endpoints: List[Endpoint]): UnionServer = {
    val unionServer = new UnionServer(dataProvider, dataReceiver, null)
    unionServer.addEndpoint(endpoints: _*)
    unionServer
  }
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
                     name: String,
                     host: String,
                     port: Int,
                     useTLS: Boolean = false,
                     credentials: Credentials = Credentials.ANONYMOUS
                   ){
  // 解析Url获取的key

  def url: String = s"${FairdClient.protocolSchema}://${host}:${port}"
  def getDftpClient(): DftpClient = {
    if(useTLS)
      FairdClient.connectTLS(url,credentials)
    else
      FairdClient.connect(url,credentials)
  }
}


