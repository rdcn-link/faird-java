package link.rdcn

import link.rdcn.TestBase._
import link.rdcn.client.FairdClient
import link.rdcn.user.UsernamePassword
import org.apache.arrow.flight.FlightRuntimeException
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

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
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", UsernamePassword("NotAdmin", adminPassword))
    )
    assertEquals("用户不存在!", exception.getMessage)
  }

  @Test()
  def testLoginWhenTokenExpired(): Unit = {
    // 模拟非admin用户的情况进行测试
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", UsernamePassword(adminUsername, "wrongPassword"))
    )
    assertEquals("无效的用户名/密码!", exception.getMessage)
  }


  @Test()
  def testAuthorizeFalse(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101", UsernamePassword(userUsername, userPassword))
    val df = dc.open(csvDir + "\\data_1.csv")
    // 假设没有权限
    val exception = assertThrows(
      classOf[Exception],
      () => df.foreach(_ => {})
    )
    assertEquals("不允许访问" + csvDir + "\\data_1.csv", exception.getMessage)
  }

  //匿名访问DataFrame失败
  @Test
  def testAnonymousAccessDataFrameFalse(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101")
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => dc.open(csvDir + "\\data_1.csv")
    )
    assertEquals("User not logged in", exception.getMessage)

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
    assertEquals("DataFrame不存在!", exception.getMessage)
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
