package link.rdcn


import link.rdcn.client.{DacpClient, FairdClient}
import link.rdcn.provider.DataProvider
import link.rdcn.server.FlightProducerImpl
import link.rdcn.struct.ValueType.{DoubleType, IntType, LongType, StringType}
import link.rdcn.struct.{CSVSource, DataFrameInfo, DataSet, DirectorySource, Row, StructType}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, TokenAuth, UsernamePassword}
import link.rdcn.util.DataUtils
import link.rdcn.util.DataUtils.listFiles
import org.apache.arrow.flight.{FlightRuntimeException, FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import TestDataGenerator.getOutputDir
import link.rdcn.ClientTest.{password, username}
import link.rdcn.user.exception._
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{PrintWriter, StringWriter}
import java.nio.file.{Files, Path, Paths}
import scala.util.Random
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.awt.Color
import java.util.concurrent.ConcurrentHashMap
import scala.io.Source

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */

object ClientTest {
  val location = Location.forGrpcInsecure(ConfigLoader.fairdConfig.getHostPosition, ConfigLoader.fairdConfig.getHostPort)
  val allocator: BufferAllocator = new RootAllocator()

  //  generalData
  TestDataGenerator.generateTestData()
  val csvDfInfos = listFiles(getOutputDir("test_output/csv").toString).map(file => {
    DataFrameInfo(file.getAbsolutePath, CSVSource(",", true), StructType.empty.add("id", IntType).add("value", DoubleType))
  })
  val binDfInfos = Seq(
    DataFrameInfo(getOutputDir("test_output/bin").toString, DirectorySource(false), StructType.binaryStructType))

  val dataSetCsv = DataSet("csv", "1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin", "2", binDfInfos.toList)

  val username = "admin"
  val password = "admin"
  val writePassword = "write"
  val credentials: Credentials = new UsernamePassword(username, password)
  val authprovider = new AuthProvider{

    private val authenticatedUserMap = new ConcurrentHashMap[String, AuthenticatedUser]()

    /**
     * 用户认证，成功返回认证后的用户信息，失败抛出 AuthException 异常
     */
    override def authenticate(credentials: Credentials): AuthenticatedUser = {
      if(credentials.isInstanceOf[UsernamePassword]) {
        val usernamePassword =credentials.asInstanceOf[UsernamePassword]
        if (usernamePassword.getUsername == username && usernamePassword.getPassword == password) {
          new AuthenticatedUser(username, Set("admin"),Set("read"))
        } else if (usernamePassword.getUsername == username && usernamePassword.getPassword == writePassword) {
          new AuthenticatedUser(username, Set("admin"),Set("write"))
        }
        else if(usernamePassword.getUsername != "admin" ) {
          throw new UserNotFoundException() {}
        } else {
          throw new InvalidCredentialsException()
        }
      } else {
        throw new TokenExpiredException()
      }
    }

    /**
     * 判断用户是否具有某项权限
     */
    override def authorize(user: AuthenticatedUser, dataFrameName: String): Boolean = {
      user.getPermissions.contains("write")
    }

    def putAuthenticatedUser(token: String, user: AuthenticatedUser): Unit = {
      authenticatedUserMap.put(token, user)
    }
  }
  val dataProvider = new DataProvider() {

    val authProvider: AuthProvider = authprovider

    override def setDataSets: List[DataSet] = {
      List(dataSetCsv, dataSetBin)
    }

  }


  val producer = new FlightProducerImpl(allocator, location, dataProvider)
  val flightServer = FlightServer.builder(allocator, location, producer).build()
  lazy val dc: DacpClient = FairdClient.connect("dacp://0.0.0.0:3101", username, password)

  @BeforeAll
  def startServer(): Unit = {
    //
    flightServer.start()
    println(s"Server (Location): Listening on port ${flightServer.getPort}")

  }

  @AfterAll
  def stopServer(): Unit = {
    //
    producer.close()
    flightServer.close()
    //    dc.close()

    DataUtils.closeAllFileSources()
    TestDataGenerator.cleanupTestData()
  }


  def genModel: Model = {
    ModelFactory.createDefaultModel()
  }


}

class ClientTest {
  val provider = ClientTest.dataProvider
  val csvModel: Model = ClientTest.genModel
  val binModel: Model = ClientTest.genModel
  val dc: DacpClient = ClientTest.dc
  val baseDir: String = getOutputDir("test_output").toString
  val baseCSVDir: String = baseDir+"\\csv"


  @Test()
  def testLoginWhenUsernameIsNotAdmin(): Unit = {
    // 模拟非admin用户的情况进行测试
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", "NotAdmin", password)
    )
    assertEquals("用户不存在!", exception.getMessage)
  }

  @Test()
  def testLoginWhenTokenExpired(): Unit = {
    // 模拟非admin用户的情况进行测试
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", username, "wrongpassword")
    )
    assertEquals("无效的用户名/密码!", exception.getMessage)
  }



  @Test()
  def testLoginWhenCredentialsIsInvalid(): Unit = {
    // 模拟非admin用户的情况进行测试
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", "expiredToken")
    )
    assertEquals("Token过期!", exception.getMessage)
  }

  @Test()
  def testAuthorizeFalse(): Unit = {
    val df = dc.open(baseCSVDir+"\\data_1.csv")
    // 假设没有write权限
    val exception = assertThrows(
      classOf[Exception],
      () => df.foreach(_ => {})
    )
    assertEquals("No access permission "+baseCSVDir+"\\data_1.csv", exception.getMessage)
  }

  
  @Test
  def testListDataSetNames(): Unit = {
    assertEquals(provider.listDataSetNames().toSet, dc.listDataSetNames().toSet, "ListDataSetNames接口输出与预期不符！")
  }

  @Test
  def testListDataFrameNames(): Unit = {
    assertEquals(provider.listDataFrameNames("csv").toSet, dc.listDataFrameNames("csv").toSet, "ListDataFrameNames接口读取csv文件输出与预期不符！")
    assertEquals(provider.listDataFrameNames("bin").toSet, dc.listDataFrameNames("bin").toSet, "ListDataFrameNames接口读取二进制文件输出与预期不符！")

  }

  @Test
  def testGetDataSetMetaData(): Unit = {
    //注入元数据
    provider.getDataSetMetaData("csv", csvModel)
    assertEquals(csvModel.toString, dc.getDataSetMetaData("csv"), "GetDataSetMetaData接口读取csv文件输出与预期不符！")
    provider.getDataSetMetaData("bin", binModel)
    assertEquals(binModel.toString, dc.getDataSetMetaData("bin"), "GetDataSetMetaData接口读取二进制文件输出与预期不符！")
  }


}
