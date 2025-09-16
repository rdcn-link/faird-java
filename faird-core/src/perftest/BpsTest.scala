/**
 * @Author Yomi
 * @Description:
 * @Data 2025/8/28 16:07
 * @Modified By:
 */
package link.rdcn

import link.rdcn.BpsTest.step
import link.rdcn.client.dacp.FairdClient
import link.rdcn.struct.{Blob, DataFrame, DataStreamSource, DefaultDataFrame, Row}
import link.rdcn.user.UsernamePassword
import org.apache.commons.io.IOUtils

import java.io.PrintWriter
import scala.collection.mutable.Map
import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Path, Paths}

object BpsTest {

  //测试数据文件夹
  val prefix = "/home/faird/faird-core/src/test/demo/data/"
  val dc: FairdClient = FairdClient.connect("dacp://10.0.82.147:3101", UsernamePassword("admin@instdb.cn", "admin001"))
  val step = 1000
  val stepSpeedData: Map[Int, Double] = Map()
  val stepResourceData: Map[Int,List[Double]] = Map()
  val csvPath = "/csv/data_7.csv"
  val jsonPath = "/json/million_lines.json"
  val binPath = "/bin"
  val structuredPath = "/structured/million_lines.json"
  val dirListPath = "/dir/images"
  val midListPath = "/mid"
  val randomListPath = "/random"
  val dirPath = "/dir/images"
  val speedFilePath = "/home/lzj/steps_data_batch_min_binary.csv"
  val resourceFilePath = "/home/lzj/steps_resource_batch_min_binary.csv"
  val provider = new TestDemoProvider

  def main(args: Array[String]): Unit = {
    //    testRowFileListBinary(midListPath)
    //    testRowFileListBlob(midListPath)
    //    testRowFileListBinary(randomListPath)
    //    testRowFileListBlob(randomListPath)
    //    testRowFileListBinary(dirListPath)
    //    testRowFileListBlob(dirListPath)
    //        time(testBin,binPath)
    //        time(testJson,jsonPath)
    //        time(testJsonSelect,jsonPath)
    //        time(testCsv,csvPath)
    //        testRowJson(jsonPath,step)
    //        testRowCsv(csvPath,step)
    //        testRowSelectCsv(csvPath,step)
    //        testRowSelectJson(jsonPath,step)
    //    testBinBinary(binPath)
    //    testBinBlob(binPath)
    //    testRowStructured(structuredPath)
    //    testRowSelect5Structured(structuredPath)
    testUpload()
    dumpToFile(speedFilePath,stepSpeedData)
    dumpToFile(resourceFilePath,stepResourceData)
  }

  def testUpload(): Unit = {
    //      testUploadRowStructured(jsonPath)
    //      testUploadRowStructured(dirListPath)
    //    testUploadRowStructured(midListPath)
    //    testUploadRowStructured(binPath)
    testUploadRowStructured(randomListPath)
    //      testUploadRowStructured(jsonPath)
  }


  def testRowUploadFileListBlob(name: String): Unit = {
    val dfStructured: DataFrame = dc.getByPath(name)
    timeIterator(dfStructured, step, row => {
      val name: String = row._1.asInstanceOf[String]
      val blob: Blob = row.get(6).asInstanceOf[Blob]
      val path: Path = Paths.get("faird-core","src", "test", "demo", "data", "output", name)
      blob.offerStream(inputStream => {
        val outputStream = new FileOutputStream(path.toFile)
        IOUtils.copy(inputStream, outputStream)
        outputStream.close()
      })
    })
  }

  def testUploadRowStructured(name: String): Unit = {
    val dataStreamSource: DataStreamSource = provider.dataProvider.getDataStreamSource(name)
    val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
    dc.put(dataFrame,step)
  }



  def dumpToFile[T, U](filePath: String, map: Map[T, U]): Unit = {

    // 使用 PrintWriter 将数据写入文件
    val writer = new PrintWriter(filePath)
    try {
      // 写入 CSV 文件头
      writer.println("1,2")

      // 遍历并写入数据
      map.foreach { case (step, speed) =>
        writer.println(s"$step,$speed")
      }
      println(s"数据已成功写入 $filePath")
    } finally {
      writer.close()
    }
  }



  def testRowFileListBinary(name: String): Unit = {
    val dfStructured: DataFrame = dc.getByPath(name)
    timeIterator(dfStructured, step, row => {
      val name: String = row._1.asInstanceOf[String]
      val blob: Array[Byte] = row.get(6).asInstanceOf[Array[Byte]]
      val path: Path = Paths.get("faird-core","src", "test", "demo", "data", "output", name)
      val outputStream = new FileOutputStream(path.toFile)
      IOUtils.write(blob,outputStream)
      outputStream.close()
    })
  }

  def testRowFileListBlob(name: String): Unit = {
    val dfStructured: DataFrame = dc.getByPath(name)
    timeIterator(dfStructured, step, row => {
      val name: String = row._1.asInstanceOf[String]
      val blob: Blob = row.get(6).asInstanceOf[Blob]
      val path: Path = Paths.get("faird-core","src", "test", "demo", "data", "output", name)
      //      blob.offerStream(inputStream => {
      //        val outputStream = new FileOutputStream(path.toFile)
      //        IOUtils.copy(inputStream, outputStream)
      //        outputStream.close()
      //      })
    })
  }

  def testRowStructured(name: String): Unit = {
    val dfStructured: DataFrame = dc.getByPath(name)
    timeIterator(dfStructured, step, () => _)
  }

  def testRowSelect1Structured(name: String): Unit = {
    val dfStructured: DataFrame = dc.getByPath(name)
    timeIterator(dfStructured.select("user_id"), step, () => _)
  }

  def testRowSelect2Structured(name: String): Unit = {
    val dfStructured: DataFrame = dc.getByPath(name)
    timeIterator(dfStructured.select("user_id","business_id"), step, () => _)
  }

  def testRowSelect3Structured(name: String): Unit = {
    val dfStructured: DataFrame = dc.getByPath(name)
    timeIterator(dfStructured.select("user_id","business_id","text"), step, () => _)
  }

  def testRowSelect4Structured(name: String): Unit = {
    val dfStructured: DataFrame = dc.getByPath(name)
    timeIterator(dfStructured.select("user_id","business_id","text","date"), step, () => _)
  }

  def testRowSelect5Structured(name: String): Unit = {
    val dfStructured: DataFrame = dc.getByPath(name)
    timeIterator(dfStructured.select("user_id","business_id","text","date","compliment_count"), step, () => _)
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

  def testBinBinary(name: String): Unit = {
    val dfBin: DataFrame = dc.getByPath(name)
    timeIteratorFull(dfBin, step, row => {
      val name: String = row._1.asInstanceOf[String]
      val blob = row.getAs[Array[Byte]](6)
      val path: Path = Paths.get("faird-core","src", "test", "demo", "data", "output", name)
      val outputStream = new FileOutputStream(path.toFile)
      IOUtils.write(blob,outputStream)
      outputStream.close()
    })
  }

  def testBinBlob(name: String): Unit = {
    val dfBin: DataFrame = dc.getByPath(name)
    timeIteratorFull(dfBin, step, row => {
      val blob = row.getAs[Blob](6)
      blob.offerStream(inputStream => {
        val name: String = row._1.asInstanceOf[String]
        val path: Path = Paths.get("faird-core","src", "test", "demo", "data", "output", name)
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
    timeIterator(dfJson.select("user_id"), step, () => _)
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
    val numberRegex = "(\\d+)".r
    var start = end
    var sum_duration = 0L
    val info = dc.getServerResourceInfo
    //    print(info)
    stepResourceData.put(0,List(getNum(info.getOrElse("jvm.memory.used.mb","0"))/getNum(info.getOrElse("jvm.memory.total.mb","0")),
      getNum(info.getOrElse("cpu.usage.percent","0"))))
    it.foreach { item =>
      op(item)
      count += 1

      if (count % step == 0) {
        start = end
        end = System.currentTimeMillis()
        val duration = (end - start)
        sum_duration += duration
        println(s"Processed $count rows in ${duration/ 1000.0} s. Average speed: ${(step.toDouble / duration *1000).toLong} rows/sec.")
        stepSpeedData.put(count / step, step.toDouble / duration *1000 )
        val info = dc.getServerResourceInfo
        stepResourceData.put(count / step,List(getNum(info.getOrElse("jvm.memory.used.mb","0"))/getNum(info.getOrElse("jvm.memory.total.mb","0")),
          getNum(info.getOrElse("cpu.usage.percent","0"))))
      }
    }
    end = System.currentTimeMillis()
    println(s"Average speed: ${(1000000.0 / sum_duration *1000).toLong} rows/sec.")
    println(s"Max speed: ${stepSpeedData.maxBy(_._2)} ×10^4 rows/sec.")


    // 打印最终总耗时
    //    end = System.currentTimeMillis()
    //    val totalDuration = (end - start) / 1000.0
    //    println(s"Total processed $count rows in ${totalDuration} s.")
  }

  def timeIteratorFull(
                        it: DataFrame,
                        step: Int,
                        op: Row => Unit
                      ): Unit = {

    var count = 0
    var end = System.currentTimeMillis()
    val numberRegex = "(\\d+)".r
    var start = end
    var sum_duration = 0L
    it.foreach { item =>
      op(item)
    }
    end = System.currentTimeMillis()
    val totalDuration = (end - start) / 1000.0
    println(s"Total processed $count rows in ${totalDuration} s.")
  }

  def getNum(str: String): Double = {
    val numberRegex = "(\\d+)".r
    val matchOption = numberRegex.findFirstMatchIn(str)

    val result: Option[Double] = matchOption.map { m =>
      m.group(1).toDouble
    }
    result match {
      case Some(value) => value
      case None        => 0.0
    }
  }

}
