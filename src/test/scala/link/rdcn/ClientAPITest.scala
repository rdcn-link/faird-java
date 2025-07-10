package link.rdcn

import link.rdcn.TestBase._
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeAll, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */


class ClientAPITest extends TestBase {
  val csvModel: Model = genModel
  val binModel: Model = genModel

  @Test
  def testListDataSetNames(): Unit = {
    assertEquals(dataProvider.listDataSetNames().toSet, dc.listDataSetNames().toSet, "ListDataSetNames接口输出与预期不符！")
  }

  @Test
  def testListDataFrameNames(): Unit = {
    assertEquals(dataProvider.listDataFrameNames("csv").toSet, dc.listDataFrameNames("csv").toSet, "ListDataFrameNames接口读取csv文件输出与预期不符！")
    assertEquals(dataProvider.listDataFrameNames("bin").toSet, dc.listDataFrameNames("bin").toSet, "ListDataFrameNames接口读取二进制文件输出与预期不符！")

  }

  @Test
  def testGetDataSetMetaData(): Unit = {
    //注入元数据
    dataProvider.getDataSetMetaData("csv", csvModel)
    assertEquals(csvModel.toString, dc.getDataSetMetaData("csv"), "GetDataSetMetaData接口读取csv文件输出与预期不符！")
    dataProvider.getDataSetMetaData("bin", binModel)
    assertEquals(binModel.toString, dc.getDataSetMetaData("bin"), "GetDataSetMetaData接口读取二进制文件输出与预期不符！")
  }

  @Test
  def testGetSchema(): Unit = {
    //注入元数据
    dataProvider.getDataSetMetaData("csv", csvModel)
    assertEquals(csvModel.toString, dc.getDataSetMetaData("csv"), "GetDataSetMetaData接口读取csv文件输出与预期不符！")
    dataProvider.getDataSetMetaData("bin", binModel)
    assertEquals(binModel.toString, dc.getDataSetMetaData("bin"), "GetDataSetMetaData接口读取二进制文件输出与预期不符！")
  }

  @Test
  def testGetHostInfo(): Unit = {
    assertEquals(expectedHostInfo, dc.getHostInfo, "GetHostInfo接口输出与预期不符！")
  }

  @ParameterizedTest
  @ValueSource(strings = Array("/csv/data_1.csv"))
  def testGetDataFrameSize(dataFrameName: String): Unit = {
    assertEquals(getExpectedDataFrameSize(dataFrameName), dc.getDataFrameSize(dataFrameName), "GetDataFrameSize接口输出与预期不符！")
  }

  @Test
  def testGetServerResourceInfo(): Unit = {
    val statusString = dc.getServerResourceInfo()

    assertNotNull(statusString)
    assertTrue(statusString.contains("服务器资源使用情况:"))
    assertTrue(statusString.contains("CPU核心数"))
    assertTrue(statusString.contains("CPU使用率"))
    assertTrue(statusString.contains("JVM内存 (MB):"))
    assertTrue(statusString.contains("系统物理内存 (MB):"))

    assertTrue(statusString.matches("(?s).*CPU核心数\\s*:\\s*\\d+.*"))
    assertTrue(statusString.matches("(?s).*CPU使用率\\s*:\\s*\\d+\\.\\d{2}%.*"))
  }


}
