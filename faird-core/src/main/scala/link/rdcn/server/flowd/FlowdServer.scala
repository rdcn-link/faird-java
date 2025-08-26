package link.rdcn.server.flowd

import link.rdcn.client.UrlValidator
import link.rdcn.client.dacp.FairdClient
import link.rdcn.client.dftp.DftpClient
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics, DataProvider}
import link.rdcn.received.DataReceiver
import link.rdcn.server.dacp.DacpServer
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame, Row}
import link.rdcn.user.{Credentials, AuthProvider}
import org.apache.jena.rdf.model.Model

import java.util
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/20 13:57
 * @Modified By:
 */
class FlowdServer private(dataProvider: DataProvider, dataReceiver: DataReceiver, authProvider: AuthProvider) extends DacpServer(dataProvider, dataReceiver, authProvider)  {

  private val endpoints = mutable.ListBuffer[Endpoint]()

  private val endpointClientsMap = mutable.Map[String, DftpClient]()

  def addEndpoint(endpoints: Endpoint*): Unit = {
    endpoints.foreach(endpoint => {
      this.endpointClientsMap.put(endpoint.url, endpoint.getDftpClient())
      this.endpoints.append(endpoint)
    })
  }

  def getEndpoints(): List[Endpoint] = endpoints.toList

  override def doListDataSets(): DataFrame = {
    val dataFrames = endpointClientsMap.toList.map(kv => kv._2.get(s"${kv._1}/listDataSets"))
    mergeDataFrame(dataFrames)
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

  private def mergeDataFrame(dataFrames: List[DataFrame]): DataFrame = {
    val stream: Iterator[Row]= dataFrames.map(df => df.mapIterator[Iterator[Row]](iter => iter)).reduce(_ ++ _)
    val schema = dataFrames.head.schema
    DefaultDataFrame(schema, stream)
  }
}

object FlowdServer{

  def apply(endpoints: List[Endpoint]): FlowdServer = {
    val flowdServer = new FlowdServer(dataProvider, dataReceiver, null)
    flowdServer.addEndpoint(endpoints: _*)
    flowdServer
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
