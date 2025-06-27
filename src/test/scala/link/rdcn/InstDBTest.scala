package link.rdcn

import link.rdcn.client.FairdClient
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.junit.jupiter.api.Test

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/27 17:14
 * @Modified By:
 */
class InstDBTest {

  @Test
  def m1(): Unit = {
    val dc = FairdClient.connect("dacp://10.0.82.71:8232")
    dc.listDataSetNames().foreach(ds => println(s"数据集名称：$ds"))
    println("---------------------------------------------------------------------------")
    dc.listDataFrameNames("滚动轴承基础物理参数检测技术").foreach(println)
//    dc.listDataFrameNames("复杂装配体刚强度求解器").foreach(println)
//    dc.listDataFrameNames("复杂装配体刚强度求解器").foreach(println)

//    val model: Model = ModelFactory.createDefaultModel()
    println(dc.getDataSetMetaData("滚动轴承基础物理参数检测技术"))

    val df = dc.open("/mnt/sdb/instdb/滚动轴承基础物理参数检测技术.csv")
    df.foreach(println)
//    val df1 = dc.open("/mnt/sdb/instdb/log")
//    df1.foreach(println)
  }

}
