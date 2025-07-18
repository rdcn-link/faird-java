package link.rdcn

import link.rdcn.FairdConfigKeys._
import link.rdcn.TestBase._
import org.apache.jena.rdf.model.Model
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.JavaConverters.mapAsScalaMapConverter
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
    val allKeys: Set[String] = Set(
      faird_host_name,
      faird_host_port,
      faird_host_title,
      faird_host_position,
      faird_host_domain,
      faird_tls_enabled,
      faird_tls_cert_path,
      faird_tls_key_path
    )
    val hostInfo = dc.getHostInfo().asScala.toMap
    allKeys.foreach(key =>{
      assertTrue(hostInfo.contains(key), s"实际结果中缺少键：$key")
      assertEquals(expectedHostInfo(key), hostInfo(key), s"键 '$key' 的值与预期不符！")
    }
    )
  }

  @ParameterizedTest
  @ValueSource(strings = Array("/csv/data_1.csv"))
  def testGetDataFrameSize(dataFrameName: String): Unit = {
    assertEquals(getExpectedDataFrameSize(dataFrameName), dc.getDataFrameSize(dataFrameName), "GetDataFrameSize接口输出与预期不符！")
  }

  @Test
  def testGetServerResourceInfo(): Unit = {
    val statusMap = dc.getServerResourceInfo()

    assertNotNull(statusMap)
    assertTrue(statusMap.containsKey("cpu.cores"))
    assertTrue(statusMap.containsKey("cpu.usage.percent"))
    assertTrue(statusMap.containsKey("jvm.memory"))
    assertTrue(statusMap.containsKey("system.physical.memory"))
  }


}
