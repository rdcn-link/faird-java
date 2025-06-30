package link.rdcn


import link.rdcn.ClientTest._
import link.rdcn.client.FairdClient
import link.rdcn.provider.DataProvider
import link.rdcn.server.FlightProducerImpl
import link.rdcn.struct.ValueType.{DoubleType, IntType, LongType, StringType}
import link.rdcn.struct.{CSVSource, DataFrameInfo, DataSet, DirectorySource, Row, StructType}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, TokenAuth, UsernamePassword}
import link.rdcn.util.{DataGenerator, DataUtils}
import link.rdcn.util.DataUtils.listFiles
import org.apache.arrow.flight.{FlightRuntimeException, FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import link.rdcn.util.DataGenerator.getOutputDir
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
import scala.collection.JavaConverters.{seqAsJavaListConverter, setAsJavaSetConverter}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
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
  DataGenerator.generateTestData()
  val csvDfInfos = listFiles(getOutputDir("test_output/csv").toString).map(file => {
    DataFrameInfo(file.getAbsolutePath, CSVSource(",", true), StructType.empty.add("id", IntType).add("value", DoubleType))
  })
  val binDfInfos = Seq(
    DataFrameInfo(getOutputDir("test_output/bin").toString, DirectorySource(false), StructType.binaryStructType))

  val dataSetCsv = DataSet("csv", "1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin", "2", binDfInfos.toList)

  val adminUsername = "admin"
  val adminPassword = "admin"
  val userUsername = "user"
  val userPassword = "user"
//  val credentials: Credentials = new UsernamePassword(username, password)
  val authprovider = new AuthProvider {


    /**
     * 用户认证，成功返回认证后的用户信息
     */
    override def authenticate(credentials: Credentials): AuthenticatedUser = {
      if (credentials.isInstanceOf[UsernamePassword]) {
        val usernamePassword = credentials.asInstanceOf[UsernamePassword]
        if (usernamePassword.getUsername == adminUsername && usernamePassword.getPassword == adminPassword) {
          new AuthenticatedUser(adminUsername, Set("admin").asJava, Set.empty.asJava)
        } else if (usernamePassword.getUsername == userUsername && usernamePassword.getPassword == userPassword) {
          new AuthenticatedUser(userUsername, Set("user").asJava, Set.empty)
        }
        else if (usernamePassword.getUsername != "admin") {
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
      if (user.getUserId == "admin") true
      else user.getPermissions.contains(s"$dataFrameName")
    }

  }
  val dataProvider = new DataProvider() {

    val authProvider: AuthProvider = authprovider

    override def setDataSets: java.util.List[DataSet] = {
      List(dataSetCsv, dataSetBin).asJava
    }

  }


  val producer = new FlightProducerImpl(allocator, location, dataProvider)
  val flightServer = FlightServer.builder(allocator, location, producer).build()
  lazy val dc = FairdClient.connect("dacp://0.0.0.0:3101", adminUsername, adminPassword)

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
    DataGenerator.cleanupTestData()
  }


  def genModel: Model = {
    ModelFactory.createDefaultModel()
  }


}

class ClientTest {
  val provider = ClientTest.dataProvider
  val csvModel: Model = ClientTest.genModel
  val binModel: Model = ClientTest.genModel
  val dc = ClientTest.dc
  val baseDir: String = getOutputDir("test_output").toString
  val baseCSVDir: String = baseDir + "\\csv"


  @Test()
  def testLoginWhenUsernameIsNotAdmin(): Unit = {
    // 模拟非admin用户的情况进行测试
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", "NotAdmin", adminPassword)
    )
    assertEquals("用户不存在!", exception.getMessage)
  }

  @Test()
  def testLoginWhenTokenExpired(): Unit = {
    // 模拟非admin用户的情况进行测试
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", adminUsername, "wrongPassword")
    )
    assertEquals("无效的用户名/密码!", exception.getMessage)
  }



  @Test()
  def testAuthorizeFalse(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101", userUsername, userPassword)
    val df = dc.open(baseCSVDir + "\\data_1.csv")
    // 假设没有权限
    val exception = assertThrows(
      classOf[Exception],
      () => df.foreach(_ => {})
    )
    assertEquals("不允许访问" + baseCSVDir + "\\data_1.csv", exception.getMessage)
  }
  //匿名访问


  @Test
  def testListDataSetNames(): Unit = {
    assertEquals(provider.listDataSetNames().toSet, dc.listDataSetNames().toSet, "ListDataSetNames接口输出与预期不符！")
  }

  @Test
  def testListDataFrameNames(): Unit = {
    assertEquals(provider.listDataFrameNames("csv").toSet, dc.listDataFrameNames("csv").toSet, "ListDataFrameNames接口读取csv文件输出与预期不符！")
    assertEquals(provider.listDataFrameNames("bin").toSet, dc.listDataFrameNames("bin").toSet, "ListDataFrameNames接口读取二进制文件输出与预期不符！")

  }
  //服务未启动
  //df不存在
  //访问时

  @Test
  def testGetDataSetMetaData(): Unit = {
    //注入元数据
    provider.getDataSetMetaData("csv", csvModel)
    assertEquals(csvModel.toString, dc.getDataSetMetaData("csv"), "GetDataSetMetaData接口读取csv文件输出与预期不符！")
    provider.getDataSetMetaData("bin", binModel)
    assertEquals(binModel.toString, dc.getDataSetMetaData("bin"), "GetDataSetMetaData接口读取二进制文件输出与预期不符！")
  }


}
