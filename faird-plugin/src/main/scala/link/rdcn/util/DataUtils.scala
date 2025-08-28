package link.rdcn.util

import link.rdcn.Logging
import link.rdcn.struct.ValueType._
import link.rdcn.struct._
import org.apache.poi.ss.usermodel.{Cell, CellType, DateUtil}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.json.JSONObject

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import scala.collection.JavaConverters.{asScalaIteratorConverter, seqAsJavaListConverter}
import scala.collection.mutable
import scala.io.Source

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/19 16:03
 * @Modified By:
 */
object DataUtils extends Logging{
  private val resourceManager = new ResourceManager

  private class ResourceManager {
    private val resources = mutable.Map[String, Source]()

    def register(source: Source, filePath: String): Unit = {
      resources.synchronized {
        resources += (filePath -> source)
      }
    }

    def getSource(filePath: String): Option[Source] = {
      resources.synchronized {
        resources.get(filePath)
      }
    }

    def close(filePath: String): Unit = {
      resources.synchronized {
        resources.get(filePath).foreach { source =>
          source.close()
          resources -= filePath
        }
      }
    }

    def closeAll(): Unit = {
      resources.synchronized {
        resources.values.foreach(_.close())
        resources.clear()
        System.gc()
        Thread.sleep(100)
      }
    }
  }

  def getStructTypeFromMap(row: Map[String, Any]): StructType = {
    StructType(row.map(row => Column(row._1, inferValueType(row._2))).toSeq)
  }

  def getStructTypeStreamFromJson(iter: Iterator[String]): (Iterator[Row], StructType) = {
    if(iter.hasNext){
      val firstLine = iter.next()
      val jo = new JSONObject(firstLine)
      val structType: StructType = StructType(jo.keys().asScala.map(key => Column(key, inferValueType(jo.get(key)))).toSeq)
      val stream: Iterator[Row] = iter.map(Row.fromJsonString(_)) ++ Seq(Row.fromJsonString(firstLine)).iterator
      (stream, structType)
    }else (Iterator.empty, StructType.empty)
  }

  def convertStringRowToTypedRow(row: Row, schema: StructType): Row = {
    val typedValues = schema.columns.zipWithIndex.map { case (field, i) =>
      val rawValue = row.getAs[String](i) // 原始 String 值
      if (rawValue == null) {
        null
      } else field.colType match {
        case IntType    => rawValue.toInt
        case LongType       => rawValue.toLong
        case DoubleType     => rawValue.toDouble
        case FloatType      => rawValue.toFloat
        case BooleanType    => rawValue.toBoolean
        case StringType     => rawValue
        // 你可以继续扩展其他类型
        case _ => throw new UnsupportedOperationException(s"Unsupported type: ${field.colType}")
      }
    }
    Row.fromSeq(typedValues)
  }



  def inferSchemaFromRow(row: Row): StructType = {
    val columns = row.values.zipWithIndex.map { case (value, idx) =>
      val name = s"_${idx + 1}"
      val valueType = inferValueType(value)
      Column(name, valueType)
    }
    StructType.fromSeq(columns)
  }

  def getDataFrameByStream(stream: Iterator[Row]): DefaultDataFrame = {
    if(stream.isEmpty) return DefaultDataFrame(StructType.empty, ClosableIterator(Iterator.empty)(()=>()))
    val row = stream.next()
    val structType = inferSchemaFromRow(row)
    stream match {
      case iter: ClosableIterator[Row] => DefaultDataFrame(structType, ClosableIterator(Seq(row).iterator ++ stream)(iter.close))
      case _ => DefaultDataFrame(structType, ClosableIterator(Seq(row).iterator ++ stream)(()=>()))
    }
  }

//  /** 推断一个值的类型 */
  def inferStringValueType(value: String): ValueType = {
    if (value == null || value.isEmpty) StringType
    else if (value.matches("^-?\\d+$")) LongType
    else if (value.matches("^-?\\d+\\.\\d+$")) DoubleType
    else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) BooleanType
    else StringType
  }

  def inferValueType(value: Any): ValueType = value match {
    case null                   => NullType
    case _: Int                 => IntType
    case _: Long                => LongType
    case _: Double | _: Float   => DoubleType
    case _: Boolean             => BooleanType
    case _: Array[Byte]         => BinaryType
    case _: java.io.File        => BinaryType
    case _: Blob                => BlobType
    case _                      => StringType
  }

  /** 推断多列的类型（每列保留最大兼容类型） */
  def inferSchema(lines: Seq[Array[String]], header: Seq[String]): StructType = {
    if (lines.isEmpty)
      return StructType.empty

    val numCols = lines.head.length

    // 如果 header 为空，则自动生成 col_0, col_1, ...
    val columnNames: Seq[String] =
      if (header.isEmpty)
        Array.tabulate(numCols)(i => s"_${i + 1}")
      else
        header

    // transpose: 行列互换以便按列推断类型
    val transposed: Seq[Seq[String]] = lines.transpose.map(_.toSeq)

    val types: Seq[ValueType] = transposed.map { colValues =>
      val guessedTypes = colValues.map(inferStringValueType)
      if (guessedTypes.contains(StringType)) StringType
      else if (guessedTypes.contains(DoubleType)) DoubleType
      else if (guessedTypes.contains(LongType)) LongType
      else BooleanType
    }

    StructType.fromSeq(columnNames.zip(types).map { case (name, vt) => Column(name.trim, vt) })
  }

  /** 底层流式统计一个file的行数 */
  def countLinesFast(file: File): Long = {
    val reader = Files.newBufferedReader(file.toPath, StandardCharsets.UTF_8)
    try {
      reader.lines().count()
    } finally {
      reader.close()
    }
  }

  // 列出目录下所有文件
  def listFiles(directoryPath: String): Seq[File] = {
    val dir = new File(directoryPath)
    if (dir.exists() && dir.isDirectory) {
      dir.listFiles().filter(_.isFile).toSeq
    } else {
      Seq.empty
    }
  }

  def listAllFilesWithAttrs(directoryFile: File): Iterator[(File, BasicFileAttributes)] = {
    def walk(file: File): Iterator[File] = {
      if (file.isDirectory) {
        // 避免 null 的情况：listFiles 返回 null 时用空数组代替
        Option(file.listFiles()).getOrElse(Array.empty).iterator.flatMap(walk)
      } else if (file.isFile) {
        Iterator.single(file)
      } else {
        Iterator.empty
      }
    }

    walk(directoryFile).flatMap { file =>
      val path = file.toPath
      try {
        val attrs = Files.readAttributes(path, classOf[BasicFileAttributes])
        Some((file, attrs))
      } catch {
        case _: IOException =>
          logger.error(s"读取文件 ${file.getAbsolutePath} 失败")
          None
      }
    }
  }

  def listFilesWithAttributes(directoryFile: File): Seq[(File, BasicFileAttributes)] = {
    if (directoryFile.exists() && directoryFile.isDirectory) {
      directoryFile.listFiles()
        .filter(_.isFile)
        .toSeq
        .flatMap { file =>
          val path = file.toPath
          try {
            val attrs = Files.readAttributes(path, classOf[BasicFileAttributes])
            Some((file, attrs))
          } catch {
            case _: IOException =>
              logger.error(s"读取文件${file.getAbsolutePath} 失败")
              None
          }
        }
    } else {
      Seq.empty
    }
  }

  def getFileTypeByExtension(file: File): String = {
    val fileName = file.getName
    fileName.substring(fileName.lastIndexOf('.') + 1).toLowerCase match {
      case "txt"  => "Text File"
      case "jpg"  => "Image File"
      case "png"  => "Image File"
      case "pdf"  => "PDF Document"
      case "csv"  => "CSV File"
      case _      => "Unknown Type"
    }
  }

  def groupedLines(filePath: String, batchSize: Int): Iterator[Seq[String]] = {
    val source = Source.fromFile(filePath)
    val iter = source.getLines()

    // 创建包装的 Iterator 来确保文件在迭代结束后被关闭
    new Iterator[Seq[String]] {
      override def hasNext: Boolean = {
        val hn = iter.hasNext
        if (!hn) source.close()
        hn
      }

      override def next(): Seq[String] = iter.take(batchSize).toSeq
    }
  }


  def getFileLines(filePath: String): Iterator[String] = {
    val source = Source.fromFile(filePath)
    resourceManager.register(source, filePath)
    source.getLines()
  }

  def getFileLines(file: File): ClosableIterator[String] = {
    val source = Source.fromFile(file)
    ClosableIterator(source.getLines())(source.close())
  }

  def closeFileSource(filePath: String): Unit = {
    resourceManager.close(filePath)
  }

  def closeAllFileSources(): Unit = {
    resourceManager.closeAll()
  }

  def readFileInChunks(file: File, chunkSize: Int = 5 * 1024 * 1024): Iterator[Array[Byte]] = {
    val inputStream = new FileInputStream(file)
    var isClosed = false

    new Iterator[Array[Byte]] {
      override def hasNext: Boolean = {
        if (isClosed) return false

        try {
          val available = inputStream.available() > 0
          if (!available) {
            closeStream()
          }
          available
        } catch {
          case e: IOException =>
            logger.error(s"Error checking stream availability: ${e.getMessage}")
            closeStream()
            false
        }
      }

      override def next(): Array[Byte] = {
        if (isClosed) throw new NoSuchElementException("Stream already closed")

        try {
          val bufferSize = Math.min(chunkSize, inputStream.available())
          val buffer = new Array[Byte](bufferSize)
          val bytesRead = inputStream.read(buffer)

          if (bytesRead == -1) {
            closeStream()
            throw new NoSuchElementException("End of stream reached")
          } else if (bytesRead < buffer.length) {
            closeStream()
            buffer.take(bytesRead)
          } else {
            buffer
          }
        } catch {
          case e: IOException =>
            closeStream()
            throw new RuntimeException(s"Error reading from stream: ${e.getMessage}", e)
        }
      }

      private def closeStream(): Unit = {
        if (!isClosed) {
          try {
            inputStream.close()
          } catch {
            case e: IOException =>
              logger.error(s"Error closing stream: ${e.getMessage}")
          } finally {
            isClosed = true
          }
        }
      }
    }
  }

  case class ExcelResult(schema: StructType, rows: Iterator[List[Any]])

  /** 推断 schema，只读取前两行 */
  def inferExcelSchema(path: String): StructType = {
    val workbook = new XSSFWorkbook(new FileInputStream(path))
    val sheet = workbook.getSheetAt(0)
    val rowIter = sheet.iterator().asScala

    if (!rowIter.hasNext) throw new RuntimeException("Empty Excel file")
    val headerRow = rowIter.next()
    val headers = headerRow.cellIterator().asScala.map(_.toString.trim).toList

    if (!rowIter.hasNext) throw new RuntimeException("No data row to infer types")
    val typeSampleRow = rowIter.next()
    val inferredTypes = typeSampleRow.cellIterator().asScala.toList.map(detectType)

    val finalTypes = headers.zipAll(inferredTypes, "", ValueType.StringType).map(_._2)
    StructType.fromSeq(headers.zip(finalTypes).map { case (n, t) => Column(n, t) })
  }

  /** 按 schema 读取所有数据为 Iterator[List[Any]] */
  def readExcelRows(path: String, schema: StructType): Iterator[List[Any]] = {
    val workbook = new XSSFWorkbook(new FileInputStream(path))
    val sheet = workbook.getSheetAt(0)
    val rowIter = sheet.iterator().asScala

    if (!rowIter.hasNext) throw new RuntimeException("Empty Excel file")
    rowIter.next() // 跳过 header 行

    val headers = schema.columnNames
    val types = schema.columns.map(_.colType)

    rowIter.map { row =>
      headers.indices.map { idx =>
        val cell = row.getCell(idx, org.apache.poi.ss.usermodel.Row.MissingCellPolicy.RETURN_BLANK_AS_NULL)
        if (cell == null) "" else readCell(cell, types(idx))
      }.toList
    }
  }

  private def detectType(cell: Cell): ValueType = {
    cell.getCellType match {
      case CellType.NUMERIC =>
        if (DateUtil.isCellDateFormatted(cell)) ValueType.LongType
        else {
          val v = cell.getNumericCellValue
          if (v == v.toInt) ValueType.IntType
          else if (v == v.toLong) ValueType.LongType
          else ValueType.DoubleType
        }
      case CellType.BOOLEAN => ValueType.BooleanType
      case _ => ValueType.StringType
    }
  }

  private def readCell(cell: Cell, valueType: ValueType): Any = {
    valueType match {
      case ValueType.IntType     => cell.getNumericCellValue.toInt
      case ValueType.LongType    => cell.getNumericCellValue.toLong
      case ValueType.DoubleType  => cell.getNumericCellValue
      case ValueType.BooleanType => cell.getBooleanCellValue
      case _                     => cell.toString.trim
    }
  }

  def chunkedIterator(in: InputStream, chunkSize: Int = 10 * 1024 * 1024): Iterator[Array[Byte]] = new Iterator[Array[Byte]] {
    private var finished = false

    override def hasNext: Boolean = !finished

    override def next(): Array[Byte] = {
      if (finished) throw new NoSuchElementException("No more data")

      val buffer = new Array[Byte](chunkSize)
      var bytesRead = 0
      while (bytesRead < chunkSize) {
        val read = in.read(buffer, bytesRead, chunkSize - bytesRead)
        if (read == -1) {
          finished = true
          // 返回实际读到的长度
          return if (bytesRead == 0) Iterator.empty.next() else buffer.take(bytesRead)
        }
        bytesRead += read
      }
      buffer
    }
  }
}
