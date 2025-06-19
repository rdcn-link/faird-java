package org.grapheco

import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.grapheco.client.{Blob, CSVSource, DirectorySource, FairdClient}
import org.apache.spark.sql.types.{StringType, StructType}
import org.grapheco.server.{FairdServer, FlightProducerImpl}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/18 17:24
 * @Modified By:
 */
object ClientTest extends Logging {
  val location = Location.forGrpcInsecure("0.0.0.0", 33333)
  val allocator: BufferAllocator = new RootAllocator()
  val producer = new FlightProducerImpl(allocator, location)
  val flightServer = FlightServer.builder(allocator, location, producer).build()
  @BeforeAll
  def startServer(): Unit = {
    flightServer.start()
    println(s"Server (Location): Listening on port ${flightServer.getPort}")
  }
  @AfterAll
  def stopServer(): Unit = {
    producer.close()
    flightServer.close()
  }
}

class ClientTest {

  @Test
  def bpsTest(): Unit = {
    import org.apache.spark.sql.types._

    val schema = new StructType()
      .add("name", StringType)

    val dc = FairdClient.connect("dacp://0.0.0.0:33333")
    val df = dc.open("C:\\Users\\Yomi\\Downloads\\数据\\others","1.csv", schema)
    var totalBytes: Long = 0L
    var realBytes: Long = 0L
    var count: Int = 0
    val batchSize = 30000
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
  def binaryFilesTest(): Unit = {
    import org.apache.spark.sql.types._

    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("bin", BinaryType)

    val dc = FairdClient.connect("dacp://0.0.0.0:33333")
    val df = dc.open("C:\\Users\\Yomi\\Downloads\\数据\\cram","", schema,DirectorySource(false))
    var totalBytes: Long = 0L
    var realBytes: Long = 0L
    var count: Int = 0
    val batchSize = 1
    val startTime = System.currentTimeMillis()
    var start = System.currentTimeMillis()
    df.foreach(row => {
      //      计算当前 row 占用的字节数（UTF-8 编码）
      val index = row.get(0).asInstanceOf[Int]
      val name = row.get(1).asInstanceOf[String]
      val blob = row.get(2).asInstanceOf[Blob]
      //      val bytesLen = blob.length
      val bytesLen = blob.size
      println(f"Received: ${blob.chunkCount} chunks, index:$index, name:$name")
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
  def listDataSetFrameTest(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:33333")
    dc.listDataSetNames().foreach(println)
    dc.listDataFrameNames("df").foreach(println)
  }
}
