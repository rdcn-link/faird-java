package link.rdcn

import link.rdcn.TestBase._
import link.rdcn.client.FairdClient
import link.rdcn.user.UsernamePassword
import link.rdcn.user.exception._
import org.apache.arrow.flight.FlightRuntimeException
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.io.IOException
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */


class ClientTest extends TestBase {
  val csvModel: Model = genModel
  val binModel: Model = genModel


  @Test()
  def testLoginWhenUsernameIsNotAdmin(): Unit = {
    // 模拟非admin用户的情况进行测试
    val exception = new ClientException(assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", UsernamePassword("NotAdmin", adminPassword))
    ))
    assertEquals(ErrorCode.USER_NOT_FOUND, exception.getErrorCode)
  }

  @Test()
  def testLoginWhenTokenExpired(): Unit = {
    // 模拟非admin用户的情况进行测试
    val exception = new ClientException(assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", UsernamePassword(adminUsername, "wrongPassword"))
    ))

    assertEquals(ErrorCode.INVALID_CREDENTIALS, exception.getErrorCode)
  }

  @Test()
  def testAddressAlreadyInUse(): Unit = {
    val exception = new ClientException(assertThrows(
      classOf[IOException],
      ()=>flightServer.start()
    ))
    assertEquals(ErrorCode.SERVER_ADDRESS_ALREADY_IN_USE, exception.getErrorCode)
  }

  @Test()
  def testInvalidError(): Unit = {
    val exception = new ClientException(new Exception(ErrorCode.INVALID_ERROR_CODE))
    assertEquals(ErrorCode.NO_SUCH_ERROR, exception.getErrorCode)
  }

  @Test()
  def testServerAlreadyStarted(): Unit = {
    val exception = new ClientException(assertThrows(
      classOf[IllegalStateException],
      ()=>flightServer.start()
    ))
    assertEquals(ErrorCode.SERVER_ALREADY_STARTED, exception.getErrorCode)
  }

  //匿名访问DataFrame失败
  @Test
  def testAnonymousAccessDataFrameFalse(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101")
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => dc.open(csvDir + "\\data_1.csv")
    )
    assertEquals(ErrorCode.LOGIN_REQURIED, exception.getMessage)

  }

  //df不存在
  @Test
  def testAccessInvalidDataFrame(): Unit = {
    val df = dc.open(csvDir + "\\invalid.csv") // 假设没有该文件
    // 假设没有权限
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => df.foreach(_ => {})
    )
    assertEquals(ErrorCode.DATAFRAME_NOT_EXIST, exception.getMessage)
  }



  //url错误（端口和ip）


  @Test
  def testListDataSetNames(): Unit = {
    assertEquals(dataProvider.listDataSetNames().toSet, dc.listDataSetNames().toSet, "ListDataSetNames接口输出与预期不符！")
  }

  @Test
  def testListDataFrameNames(): Unit = {
    assertEquals(dataProvider.listDataFrameNames("csv").toSet, dc.listDataFrameNames("csv").toSet, "ListDataFrameNames接口读取csv文件输出与预期不符！")
    assertEquals(dataProvider.listDataFrameNames("bin").toSet, dc.listDataFrameNames("bin").toSet, "ListDataFrameNames接口读取二进制文件输出与预期不符！")

  }


  //schema不匹配
  //访问时断开连接 server closed

  @Test
  def testGetDataSetMetaData(): Unit = {
    //注入元数据
    dataProvider.getDataSetMetaData("csv", csvModel)
    assertEquals(csvModel.toString, dc.getDataSetMetaData("csv"), "GetDataSetMetaData接口读取csv文件输出与预期不符！")
    dataProvider.getDataSetMetaData("bin", binModel)
    assertEquals(binModel.toString, dc.getDataSetMetaData("bin"), "GetDataSetMetaData接口读取二进制文件输出与预期不符！")
  }


}
