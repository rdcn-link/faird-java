package link.rdcn


import link.rdcn.DataFrameOperationTest._
import link.rdcn.TestBase._
import link.rdcn.client.RemoteDataFrame
import link.rdcn.client.dag.{SourceNode, TransformerDAG, UDFFunction}
import link.rdcn.struct._
import link.rdcn.util.ExceptionHandler
import link.rdcn.util.SharedValue.getOutputDir
import org.apache.arrow.flight.FlightRuntimeException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{InputStream, PrintWriter, StringWriter}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import scala.io.Source

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */

object DataFrameOperationTest extends TestBase {

  def getLine(row: Row): String = {
    val delimiter = ","
    row.toSeq.map(_.toString).mkString(delimiter) + '\n'
  }


  def isFolderContentsMatch(dirPath1: String, dirPath2: String): Boolean = {
    val files1 = Files.list(Paths.get(dirPath1)).sorted.toArray
    val files2 = Files.list(Paths.get(dirPath2)).sorted.toArray
    files2.zip(files1).forall { case (f1, f2) =>
      computeFileHash(f1.asInstanceOf[Path]) == computeFileHash(f2.asInstanceOf[Path])
    }
  }

  def computeFileHash(file: Path, algorithm: String = "MD5"): String = {
    val digest = MessageDigest.getInstance(algorithm)
    val buffer = new Array[Byte](8192)
    var in: InputStream = null

    try {
      in = Files.newInputStream(file)
      var bytesRead = in.read(buffer)
      while (bytesRead != -1) {
        digest.update(buffer, 0, bytesRead)
        bytesRead = in.read(buffer)
      }
      digest.digest().map(b => f"${b & 0xff}%02x").mkString
    } finally {
      if (in != null) in.close() // 确保关闭
    }
  }

}

class DataFrameOperationTest extends TestBase {
  val outputDir = getOutputDir("test_output\\output").toString


  @Test
  def testDataFrameForEach(): Unit = {
    val expectedOutput = Source.fromFile(csvDir + "\\data_1.csv").getLines().toSeq.tail.mkString("\n") + "\n"
    val df = dc.open("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    df.foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from foreach operation")
  }


  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameLimit(num: Int): Unit = {
    val expectedOutput = Source.fromFile(csvDir + "\\data_1.csv").getLines().toSeq.tail.take(num).mkString("\n") + "\n"
    val df = dc.open("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    df.limit(num).foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from limit operation")
  }
  //懒计算测试

  @ParameterizedTest
  @ValueSource(ints = Array(2))
  def testDataFrameFilter(id: Int): Unit = {
    val expectedOutput = Source.fromFile(csvDir + "\\data_1.csv").getLines().toSeq(id) + "\n"
    val df = dc.open("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    val rowFilter: Row => Boolean = (row: Row) => row.getAs[Long](0).getOrElse(-1L) == id

    //匿名函数
    df.filter(rowFilter).foreach { row =>
      printWriter.write(getLine(row))
    }
    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from filter operation")
  }

  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameMap(num: Long): Unit = {
    val expectedOutput = Source.fromFile(csvDir + "\\data_1.csv").getLines()
      .toSeq
      .tail // 跳过标题行
      .map { line =>
        val cols = line.split(",") // 按逗号拆分列
        val id = cols(0).toLong + num // 第一列转Int并+1
        s"$id,${cols.tail.mkString}" // 拼接回剩余列
      }
      .mkString("\n") + "\n"
    val df = dc.open("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    val rowMapper: Row => Row = row => Row(row.getAs[Long](0).getOrElse(-1L) + num, row.get(1))

    try {
      df.map(rowMapper).foreach { row =>
        printWriter.write(getLine(row))
      }
    } catch {
      case e: FlightRuntimeException => println(ExceptionHandler.getErrorCode(e))
    }

    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from map operation")
  }

  @Test
  def testDataFrameMapColumn(): Unit = {
    val expectedOutput = Source.fromFile(csvDir + "\\data_1.csv").getLines()
      .toSeq
      .tail // 跳过标题行
      .map { line =>
        val cols = line.split(",") // 按逗号拆分列
        s"${cols.tail.mkString}" // 拼接剩余列
      }
      .mkString("\n") + "\n"
    val df = dc.open("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)
    val rowMapper: Row => Row = row => Row(row.get(1))

    try {
      df.map(rowMapper).foreach { row =>
        printWriter.write(getLine(row))
      }
    } catch {
      case e: FlightRuntimeException => println(ExceptionHandler.getErrorCode(e))
    }

    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from map operation")
  }

  @Test
  def testDataFrameSelect(): Unit = {
    val expectedOutput = Source.fromFile(csvDir + "\\data_1.csv").getLines()
      .toSeq
      .tail // 跳过标题行
      .map { line =>
        val cols = line.split(",") // 按逗号拆分列
        s"${cols.tail.mkString}" // 拼接剩余列
      }
      .mkString("\n") + "\n"
    val df = dc.open("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)

    try {
      df.select("value").foreach { row =>
        printWriter.write(getLine(row))
      }
    } catch {
      case e: FlightRuntimeException => println(ExceptionHandler.getErrorCode(e))
    }

    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from map operation")
  }

  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameUDFTake(num: Int): Unit = {
    val lines = Source.fromFile(csvDir + "\\data_1.csv").getLines().toSeq.tail
    val expectedOutput = lines.take(num).mkString("\n") + "\n"

    val udf = new UDFFunction {
      override def transform(iter: Iterator[Row]): Iterator[Row] = {
        iter.take(num)
      }
    }

    val transformerDAG = TransformerDAG(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udf
      ),
      Map(
        "A" -> Seq("B")
      )
    )
    val dfs: Seq[RemoteDataFrame] = dc.execute(transformerDAG)
    val actualOutputs = dfs.map { df =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    assertEquals(expectedOutput, actualOutputs(0))
  }

  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameUDFFilter(num: Int): Unit = {
    val lines = Source.fromFile(csvDir + "\\data_1.csv").getLines().toSeq.tail
    val expectedOutput = lines.take(num).mkString("\n") + "\n"

    val udf = new UDFFunction {
      override def transform(iter: Iterator[Row]): Iterator[Row] = {
        iter.filter(row=>row.getAs[Long](0).get <= num)
      }
    }

    val transformerDAG = TransformerDAG(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udf
      ),
      Map(
        "A" -> Seq("B")
      )
    )
    val dfs: Seq[RemoteDataFrame] = dc.execute(transformerDAG)
    val actualOutputs = dfs.map { df =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    assertEquals(expectedOutput, actualOutputs(0))
  }

  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameUDFMap(num: Int): Unit = {
    val lines = Source.fromFile(csvDir + "\\data_1.csv").getLines().toSeq.tail
    val expectedOutput = lines.take(5).map { line =>
      val cols = line.split(",")
      val id = cols(0).toLong
      val rest = cols.tail.mkString
      s"${id + num},$rest,$num"
    }.mkString("\n") + "\n"

    val udf = new UDFFunction {
      override def transform(iter: Iterator[Row]): Iterator[Row] = {
        iter.map(row => Row.fromTuple(row.getAs[Long](0).get + num, row.get(1), num))
      }
    }

    val transformerDAG = TransformerDAG(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udf
      ),
      Map(
        "A" -> Seq("B")
      )
    )
    val dfs: Seq[RemoteDataFrame] = dc.execute(transformerDAG)
    val actualOutputs = dfs.map { df =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.limit(5).foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    assertEquals(expectedOutput, actualOutputs(0))
  }

  @ParameterizedTest
  @ValueSource(ints = Array(10))
  def testDataFrameUDFMapDAG(num: Int): Unit = {
    val lines = Source.fromFile(csvDir + "\\data_1.csv").getLines().toSeq.tail
    val (expectedOutputABCSeq, expectedOutputACSeq) = lines.take(5).map { line =>
      val cols = line.split(",")
      val id = cols(0).toLong
      val rest = cols.tail.mkString
      val lineABC = s"${id + num},$rest,$num"
      val lineAC = s"$id,$rest,$num"
      (lineABC, lineAC)
    }.unzip
    val expectedOutputABC = expectedOutputABCSeq.mkString("\n") + "\n"
    val expectedOutputAC = expectedOutputACSeq.mkString("\n") + "\n"

    val udf1 = new UDFFunction {
      override def transform(iter: Iterator[Row]): Iterator[Row] = {
        iter.map(row => Row.fromTuple(row.getAs[Long](0).get + num, row.get(1)))
      }
    }
    val udf2 = new UDFFunction {
      override def transform(iter: Iterator[Row]): Iterator[Row] = {
        iter.map(row => Row.fromTuple(row.getAs[Long](0).get, row.get(1), num))
      }
    }
    val transformerDAG = TransformerDAG(
      Map(
        "A" -> SourceNode("/csv/data_1.csv"),
        "B" -> udf1,
        "C" -> udf2
      ),
      Map(
        "A" -> Seq("B", "C"),
        "B" -> Seq("C")
      )
    )
    val dfs: Seq[RemoteDataFrame] = dc.execute(transformerDAG)
    val actualOutputs = dfs.map { df =>
      val stringWriter = new StringWriter()
      val printWriter = new PrintWriter(stringWriter)

      df.limit(5).foreach { row =>
        printWriter.write(getLine(row))
      }
      printWriter.flush()
      stringWriter.toString
    }
    assertEquals(expectedOutputABC, actualOutputs(0))
    assertEquals(expectedOutputAC, actualOutputs(1))
  }

  @Test
  def testDataFrameRowIndexAccess(): Unit = {
    val lines = Source.fromFile(csvDir + "\\data_1.csv").getLines().toSeq.tail
    val expectedOutput = lines.mkString("\n") + "\n"

    val df = dc.open("/csv/data_1.csv")
    val stringWriter = new StringWriter()
    val printWriter = new PrintWriter(stringWriter)

    try {
      df.foreach { row: Row =>
        printWriter.write(s"${row._1},${row._2}\n")
      }
    } catch {
      case e: FlightRuntimeException => println(ExceptionHandler.getErrorCode(e))
    }

    printWriter.flush()
    val actualOutput = stringWriter.toString
    assertEquals(expectedOutput, actualOutput, "Unexpected output from map operation")
  }

}
