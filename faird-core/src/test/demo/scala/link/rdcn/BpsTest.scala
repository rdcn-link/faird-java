/**
 * @Author Yomi
 * @Description:
 * @Data 2025/8/28 16:07
 * @Modified By:
 */
package link.rdcn

import link.rdcn.client.dacp.FairdClient
import link.rdcn.struct.{Blob, DataFrame, Row}
import link.rdcn.user.UsernamePassword
import org.apache.commons.io.IOUtils

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Path, Paths}

object BpsTest {

  val prefix = "/home/faird/faird/faird-core/src/test/demo/data"
  val dc: FairdClient = FairdClient.connectTLS("dacp://localhost:3101", UsernamePassword("admin@instdb.cn", "admin001"))

  def main(args: Array[String]): Unit = {
    val dc: FairdClient = FairdClient.connectTLS("dacp://localhost:3101", UsernamePassword("admin@instdb.cn", "admin001"))
    val csvPath = "/csv/data_5.csv"
    val jsonPath = "/json/million_lines.json"
    val binPath = "/bin"
    time(testBin,binPath)
//    time(testJson,jsonPath)
//    time(testJsonSelect,jsonPath)
    time(testCsv,csvPath)
  }

  def testCsv(name: String): Unit = {
    val dfCsv: DataFrame = dc.getByPath(name)
    dfCsv.collect()
  }

  def testJson(name: String): Unit = {
    val dfJson: DataFrame = dc.getByPath(name)
    dfJson.collect()
  }

  def testJsonSelect(name: String): Unit = {
    val dfJson: DataFrame = dc.getByPath(name)
    dfJson.select("id").collect()
  }

  def testBin(name: String): Unit = {
    val dfBin: DataFrame = dc.getByPath(name)

    dfBin.foreach(row=>{
      val blob = row.getAs[Blob](6)
      blob.offerStream(inputStream => {
        val path: Path = Paths.get(prefix, "bin.bin")
        val outputStream = new FileOutputStream(path.toFile)
        IOUtils.copy(inputStream, outputStream)})})
  }

  def time(codeBlock:String => Unit, name: String): Unit = {
    val filePath = Paths.get(prefix, name).toString
    val file = new File(filePath)
    if (file.exists()) {
      val sizeInBytes =  if(file.isFile) file.length()
      else 1024 * 1024 * 1024L
      val sizeInKB = sizeInBytes / 1024.0
      val sizeInMB = sizeInKB / 1024.0

      println(s"file: ${file.getName}")
      println(f"size: $sizeInMB%.2f MB")
      var startTime = System.currentTimeMillis()
      val result = codeBlock(name)
      var timeInMs = System.currentTimeMillis() - startTime
      println(f"total: $sizeInMB MB, time: ${timeInMs}")

      var timeInSeconds = timeInMs / 1000.0
      if (timeInSeconds > 0) {
        val speedInMBps = sizeInMB / timeInSeconds
        println(f"test speed: $speedInMBps%.2f MB/s")
      } else {
        println("无法计算速度，因为耗时太短。")
      }
    } else {
      println(s"文件不存在或不是一个文件: $filePath")
    }
  }
}
