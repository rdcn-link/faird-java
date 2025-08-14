package link.rdcn.server

import link.rdcn.client.FairdClient
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics, DataProvider}
import link.rdcn.struct.ValueType.{LongType, StringType}
import link.rdcn.struct.{DataFrame, DataStreamSource, Row, StructType}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, DataOperationType}
import link.rdcn.util.ClosableIterator
import link.rdcn.received.DataReceiver
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.xml.XmlBeanFactory
import org.springframework.core.io.ClassPathResource

import java.util
import java.util.Arrays

class SpringIOCServerStartTest {
  @Test
  def serverStart(): Unit = {
    val f = new XmlBeanFactory(new ClassPathResource("applicationContext.xml"))
    val dataReceiver = f.getBean("dataReceiver").asInstanceOf[DataReceiver]
    val dataProvider = f.getBean("dataProvider").asInstanceOf[DataProvider]
    val authProvider = f.getBean("authProvider").asInstanceOf[AuthProvider]
    val fairdHome = getClass.getClassLoader.getResource("").getPath

    val server: FairdServer = new FairdServer(dataProvider, authProvider, dataReceiver, fairdHome)
    server.start()
    val client = FairdClient.connect("dacp://0.0.0.0:3101", Credentials.ANONYMOUS)
    assert(client.listDataSetNames().head == "dataSet1")
    assert(client.listDataFrameNames("dataSet1").head == "dataFrame1")
  }
}

class DataReceiverTest extends DataReceiver {
  /** Called once before receiving any rows */
  override def start(): Unit = ???

  /** Called for each received batch of rows */
  override def receiveRow(dataFrame: DataFrame): Unit = ???

  /** Called after all batches are received successfully */
  override def finish(): Unit = ???
}

class DataProviderTest extends DataProvider {

  /**
   * 列出所有数据集名称
   *
   * @return java.util.List[String]
   */
  override def listDataSetNames(): util.List[String] = {
    Arrays.asList("dataSet1")
  }

  /**
   * 获取数据集的 RDF 元数据，填充到传入的 rdfModel 中
   *
   * @param dataSetId 数据集 ID
   * @param rdfModel  RDF 模型（由调用者传入，方法将其填充）
   */
override def getDataSetMetaData(dataSetId: String, rdfModel: Model): Unit = ???

  /**
   * 列出指定数据集下的所有数据帧名称
   *
   * @param dataSetId 数据集 ID
   * @return java.util.List[String]
   */
override def listDataFrameNames(dataSetId: String): util.List[String] = Arrays.asList("dataFrame1")

  /**
   * 获取数据帧的数据流
   *
   * @param dataFrameName 数据帧名（如 mnt/a.csv)
   * @return 数据流源
   */
  override def getDataStreamSource(dataFrameName: String): DataStreamSource = new DataStreamSource {
    override def rowCount: Long = -1

    override def schema: StructType = StructType.empty.add("col1", StringType)

    override def iterator: ClosableIterator[Row] = {
      val row = Row.fromSeq(Seq("name"))
      ClosableIterator(Seq(row).toIterator)()
    }
  }

  /**
   * 获取数据帧详细信息
   *
   * @param dataFrameName 数据帧名
   * @return 数据帧的DataFrameDocument
   */
  override def getDocument(dataFrameName: String): DataFrameDocument = ???

  /** *
   * 获取数据帧统计信息
   *
   * @param dataFrameName 数据帧名
   * @return 数据帧的DataFrameStatistics
   */
  override def getStatistics(dataFrameName: String): DataFrameStatistics = ???
}

class AuthorProviderTest extends AuthProvider {
  /**
   * 用户认证，成功返回认证后的保持用户登录状态的凭证
   *
   * @throws AuthorizationException
   */
  override def authenticate(credentials: Credentials): AuthenticatedUser = new AuthenticatedUser{}

  /**
   * 判断用户是否具有某项权限
   *
   * @param user          已认证用户
   * @param dataFrameName 数据帧名称
   * @param opList        操作类型列表（Java List）
   * @return 是否有权限
   */
  override def checkPermission(user: AuthenticatedUser, dataFrameName: String, opList: util.List[DataOperationType]): Boolean = true
}
