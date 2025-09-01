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

  // 检查文件是否存在且是一个文
  //  val prefix = "/home/faird/faird/faird-core/src/test/demo/data"
  //测试数据文件夹
  val prefix = "/home/faird/faird-core/src/test/demo/data/"
  val dc: FairdClient = FairdClient.connect("dacp://10.0.87.114:3101", UsernamePassword("admin@instdb.cn", "admin001"))
  val step = 1000000

  def main(args: Array[String]): Unit = {
    val csvPath = "/csv/data_7.csv"
    val jsonPath = "/json/million_lines.json"
    val binPath = "/bin"
    //    time(testBin,binPath)
    //    time(testJson,jsonPath)
    //    time(testJsonSelect,jsonPath)
    //    time(testCsv,csvPath)
    //    testRowJson(jsonPath,step)
    //    testRowCsv(csvPath,step)
    testRowSelectCsv(csvPath,step)
    //    testRowSelectJson(jsonPath,step)
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

    dfBin.foreach(row => {
      val blob = row.getAs[Blob](6)
      blob.offerStream(inputStream => {
        val path: Path = Paths.get(prefix, "bin.bin")
        val outputStream = new FileOutputStream(path.toFile)
        IOUtils.copy(inputStream, outputStream)
      })
    })
  }

  def testRowCsv(name: String, step: Int): Unit = {
    val dfCsv: DataFrame = dc.getByPath(name)
    timeIterator(dfCsv, step, () => _)
  }

  def testRowSelectCsv(name: String, step: Int): Unit = {
    val dfCsv: DataFrame = dc.getByPath(name)
    timeIterator(dfCsv.select("value"), step, () => _)
  }

  def testRowJson(name: String, step: Int): Unit = {
    val dfJson: DataFrame = dc.getByPath(name)
    timeIterator(dfJson, step, () => _)
  }

  def testRowSelectJson(name: String, step: Int): Unit = {
    val dfJson: DataFrame = dc.getByPath(name)
    timeIterator(dfJson.select("id","status","timestamp"), step, () => _)
  }

  def time(codeBlock: String => Unit, name: String): Unit = {
    val filePath = Paths.get(prefix, name).toString
    val file = new File(filePath)
    if (file.exists()) {
      val sizeInBytes = if (file.isFile) file.length()
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


  def timeIterator(
                    it: DataFrame,
                    step: Int,
                    op: Row => Unit
                  ): Unit = {

    var count = 0
    var end = System.currentTimeMillis()
    var start = end
    it.foreach { item =>
      op(item)
      count += 1

      if (count % step == 0) {
        start = end
        end = System.currentTimeMillis()
        val duration = (end - start)
        println(s"Processed $count rows in ${duration/ 1000.0} s. Average speed: ${(step.toDouble / duration *1000).toLong} rows/sec.")
      }
    }

    // 打印最终总耗时
    //    end = System.currentTimeMillis()
    //    val totalDuration = (end - start) / 1000.0
    //    println(s"Total processed $count rows in ${totalDuration} s.")
  }

}
