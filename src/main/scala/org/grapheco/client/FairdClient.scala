package org.grapheco.client

import org.grapheco.server.{RemoteDataFrameImpl, RemoteExecutor}

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 14:49
 * @Modified By:
 */
object FairdClient{

  def connect (url: String, port: Int): RemoteExecutor = new RemoteExecutor(url, port)

  def main(args: Array[String]): Unit = {
    val df: RemoteDataFrameImpl = connect("0.0.0.0", 33333).open("test")
    var totalBytes: Long = 0L
    var realBytes: Long = 0L
    var count: Int = 0
    val batchSize = 500000
    val startTime = System.currentTimeMillis()
    var start = System.currentTimeMillis()
    df.foreach(row => {
      //      计算当前 row 占用的字节数（UTF-8 编码）
      val bytesLen =
                row.get(0).asInstanceOf[Array[Byte]].length
//        row.get(0).asInstanceOf[String].getBytes(StandardCharsets.UTF_8).length

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
}
