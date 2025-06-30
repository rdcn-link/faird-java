package link.rdcn

import link.rdcn.client.FairdClient
import link.rdcn.util.DataUtils
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.junit.jupiter.api.Test

import scala.collection.JavaConverters.asScalaIteratorConverter

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
    dc.listDataFrameNames("Camera trapping data in the Gaoligong Mountains and Biluo Snow Mountains of Southwest China").foreach(println)
//    dc.listDataFrameNames("复杂装配体刚强度求解器").foreach(println)
//    dc.listDataFrameNames("复杂装配体刚强度求解器").foreach(println)

    println(dc.getDataSetMetaData("Camera trapping data in the Gaoligong Mountains and Biluo Snow Mountains of Southwest China"))
//
//    val df = dc.open("/mnt/sdb/instdb/data/resourcesFile/3c41db56157d497b9c98f5354c06a789")
//    df.limit(5).foreach(println)
    val df1 = dc.open("/mnt/sdb/instdb/data/resourcesFile/3c41db56157d497b9c98f5354c06a789/Sheet1_vhtz.xlsx")
    df1.limit(10).foreach(println)
  }

  @Test
  def m2(): Unit = {
    val schema = DataUtils.inferExcelSchema("/Users/renhao/Downloads/Sheet1.xlsx")
    println(schema)
    val iter = DataUtils.readExcelRows("/Users/renhao/Downloads/Sheet1.xlsx", schema)


    iter.asScala.foreach(println(_))
  }

}
