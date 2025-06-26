package link.rdcn

import link.rdcn.client.{Blob, FairdClient}
import link.rdcn.provider.DataProvider
import link.rdcn.server.{FairdServer, FlightProducerImpl}
import link.rdcn.struct.{DataSet, Row, StructType}
import link.rdcn.struct.ValueType.{BinaryType, IntType, StringType}
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.grapheco.TestDataGenerator
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/18 17:24
 * @Modified By:
 */
object BpsTest extends Logging {
  val location = Location.forGrpcInsecure(ConfigLoader.fairdConfig.getHostPosition, ConfigLoader.fairdConfig.getHostPort)
  val allocator: BufferAllocator = new RootAllocator()
  val producer = new FlightProducerImpl(allocator, location, new DataProvider(){
    override val dataSets: List[DataSet] = List.empty
  })
  val flightServer = FlightServer.builder(allocator, location, producer).build()
  @BeforeAll
  def startServer(): Unit = {
    //
    flightServer.start()
    println(s"Server (Location): Listening on port ${flightServer.getPort}")
  }
  @AfterAll
  def stopServer(): Unit = {

    producer.close()
    flightServer.close()

  }
}

class BpsTest {

  @Test
  def listDataSetTest(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101")
    dc.listDataSetNames().foreach(println)
    println("---------------------------------------------------------------------------")
    dc.listDataFrameNames("unstructured").foreach(println)
    println("---------------------------------------------------------------------------")
    dc.listDataFrameNames("hdfs").foreach(println)
    println("---------------------------------------------------------------------------")
    dc.listDataFrameNames("ldbc").foreach(println)
  }

  @Test
  def readBinaryTest(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101")

    //    val df = dc.open("C:\\Users\\Yomi\\PycharmProjects\\Faird\\Faird\\target\\test_output\\bin\\data_1.csv")
    val df = dc.open("\\bin")

    val cnt=0
    df.foreach(
      println
    )
  }

  @Test
  def dfApiTest(): Unit = {

    val schema = StructType.empty
      .add("id", IntType, nullable = false)
      .add("name", StringType)
      .add("bin", BinaryType)

    val dc = FairdClient.connect("dacp://0.0.0.0:33333")
    val df = dc.open("dacp://10.0.0.1/bindata")
    var totalBytes: Long = 0L
    var realBytes: Long = 0L
    var count: Int = 0
    val batchSize = 2
    val startTime = System.currentTimeMillis()
    var start = System.currentTimeMillis()

    println("SchemaURI:"+df.getSchemaURI)
    println("---------------------------------------------------------------------------")

    println("StructType:"+df.getSchema)
    println("---------------------------------------------------------------------------")
    df.foreach(row => {
      //      计算当前 row 占用的字节数（UTF-8 编码）
      //      val index = row.get(0).asInstanceOf[Int]
      val name = row.get(0).asInstanceOf[String]
      val blob = row.get(6).asInstanceOf[Blob]
      //      val bytesLen = blob.length
      val bytesLen = blob.size
      println(f"Received: ${blob.chunkCount} chunks, name:$name")
      totalBytes += bytesLen
      realBytes += bytesLen

      count += 1

      if (count % batchSize == 0) {
        val endTime = System.currentTimeMillis()
        val real_elapsedSeconds = (endTime - start).toDouble / 1000
        val total_elapsedSeconds = (endTime - startTime).toDouble / 1000
        val real_mbReceived = realBytes.toDouble / (1024 * 1024)
        val total_mbReceived = totalBytes.toDouble / (1024 * 1024)
        val bps = real_mbReceived / real_elapsedSeconds
        val obps = total_mbReceived / total_elapsedSeconds
        println(f"Received: $count rows, total: $total_mbReceived%.2f MB, speed: $bps%.2f MB/s")
        start = System.currentTimeMillis()
        realBytes = 0L
      }
    })
    println(f"total: ${totalBytes/(1024*1024)}%.2f MB, time: ${System.currentTimeMillis() - startTime}")
  }


  @Test
  def binaryFilesTest(): Unit = {

    val schema = StructType.empty
      .add("id", IntType, nullable = false)
      .add("name", StringType)
      .add("bin", BinaryType)

    val dc = FairdClient.connect("dacp://0.0.0.0:33333")

    val df = dc.open("C:\\Users\\NatsusakiYomi\\Downloads\\数据")
    var totalBytes: Long = 0L
    var realBytes: Long = 0L
    var count: Int = 0
    val batchSize = 2
    val startTime = System.currentTimeMillis()
    var start = System.currentTimeMillis()
    df.foreach(row => {
      //      计算当前 row 占用的字节数（UTF-8 编码）
//      val index = row.get(0).asInstanceOf[Int]
      val name = row.get(0).asInstanceOf[String]
      val blob = row.get(1).asInstanceOf[Blob]
      //      val bytesLen = blob.length
      val bytesLen = blob.size
      println(f"Received: ${blob.chunkCount} chunks, name:$name")
      totalBytes += bytesLen
      realBytes += bytesLen

      count += 1

      if (count % batchSize == 0) {
        val endTime = System.currentTimeMillis()
        val real_elapsedSeconds = (endTime - start).toDouble / 1000
        val total_elapsedSeconds = (endTime - startTime).toDouble / 1000
        val real_mbReceived = realBytes.toDouble / (1024 * 1024)
        val total_mbReceived = totalBytes.toDouble / (1024 * 1024)
        val bps = real_mbReceived / real_elapsedSeconds
        val obps = total_mbReceived / total_elapsedSeconds
        println(f"Received: $count rows, total: $total_mbReceived%.2f MB, speed: $bps%.2f MB/s")
        start = System.currentTimeMillis()
        realBytes = 0L
      }
    })
    println(f"total: ${totalBytes/(1024*1024)}%.2f MB, time: ${System.currentTimeMillis() - startTime}")
  }

  @Test
  def bpsTest(): Unit = {

    val schema = StructType.empty
      .add("name", StringType)

    val dc = FairdClient.connect("dacp://0.0.0.0:33333")
    val df = dc.open("/Users/renhao/Downloads/MockData/hdfs")
    var totalBytes: Long = 0L
    var realBytes: Long = 0L
    var count: Int = 0
    val batchSize = 500000
    val startTime = System.currentTimeMillis()
    var start = System.currentTimeMillis()
    df.foreach(row => {
      //      计算当前 row 占用的字节数（UTF-8 编码）
      val bytesLen =
//        row.get(0).asInstanceOf[Array[Byte]].length
              row.get(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8).length

      //          row.get(1).asInstanceOf[String].getBytes(StandardCharsets.UTF_8).length

      totalBytes += bytesLen
      realBytes += bytesLen

      count += 1

      if (count % batchSize == 0) {
        val endTime = System.currentTimeMillis()
        val real_elapsedSeconds = (endTime - start).toDouble / 1000
        val total_elapsedSeconds = (endTime - startTime).toDouble / 1000
        val real_mbReceived = realBytes.toDouble / (1024 * 1024)
        val total_mbReceived = totalBytes.toDouble / (1024 * 1024)
        val bps = real_mbReceived / real_elapsedSeconds
        val obps = total_mbReceived / total_elapsedSeconds
        println(f"Received: $count rows, total: $total_mbReceived%.2f MB, speed: $bps%.2f MB/s")
        start = System.currentTimeMillis()
        realBytes = 0L
      }
    })
    println(f"total: ${totalBytes/(1024*1024)}%.2f MB, time: ${System.currentTimeMillis() - startTime}")
  }

  @Test
  def csvSourceTest(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:33333")
    val schema = StructType.empty
      .add("col1", StringType)
      .add("col2", StringType)
    val df = dc.open("/Users/renhao/Downloads/MockData/hdfs")
    df.limit(10).foreach(row => {
      println(row)
    })
    df.limit(10).map(x=>Row("-------"+x.get(0).toString, "#########"+x.get(1).toString)).foreach(println)
    df.limit(10).map(x=>Row("-------"+x.get(0).toString, "#########"+x.get(1).toString))
      .filter(x=>x.getAs[String](0).get.startsWith("###"))
      .foreach(println)
  }
  @Test
  def csvSourceLdbcTest(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:33333")
//    id|type|name|url
    val schema = StructType.empty
      .add("id", StringType)
      .add("type", StringType)
      .add("name", StringType)
      .add("url", StringType)
    val df = dc.open("/Users/renhao/Downloads/MockData/ldbc")
    df.limit(10).foreach(println)
  }
}
