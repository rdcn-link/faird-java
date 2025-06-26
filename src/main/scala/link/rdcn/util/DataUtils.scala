package link.rdcn.util

import link.rdcn.Logging
import scala.collection.mutable

import link.rdcn.struct.ValueType._
import link.rdcn.struct.{Row, StructType}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot, VectorUnloader}

import java.io.{File, FileInputStream, IOException}
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import java.util.Collections
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.io.Source

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/19 16:03
 * @Modified By:
 */
object DataUtils extends Logging{
  private val resourceManager = new ResourceManager

  class ResourceManager {
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


  //内存中生成数据
  def createCacheBatch(arrowRoot: VectorSchemaRoot, batchLen: Int): ArrowRecordBatch = {
    arrowRoot.allocateNew()
    val vec = arrowRoot.getVector("name").asInstanceOf[VarBinaryVector]
    val rowCount = batchLen
    for (i <- 0 to (rowCount - 1)) {
      val ss =
        """
          |5e1c88487133410c80a73378c1013463 a8f7ec6584bf4d40a99e898df710a2cc-754190e62b3849c18b1fcc23e4eb2fa6
          |""".stripMargin
      vec.setSafe(i, ss.getBytes("UTF-8"))
    }
    arrowRoot.setRowCount(rowCount)
    val unloader = new VectorUnloader(arrowRoot)
    unloader.getRecordBatch
  }

  def convertStringRowToTypedRow(row: Row, schema: StructType): Row = {
    val typedValues = schema.columns.zipWithIndex.map { case (field, i) =>
      val rawValue = row.getAs[String](i).getOrElse(null) // 原始 String 值
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

  def convertStructTypeToArrowSchema(structType: StructType): Schema = {
    val fields: List[Field] = structType.columns.map { column =>
      val arrowFieldType = column.colType match {
        case IntType =>
          new FieldType(column.nullable, new ArrowType.Int(32, true), null)
        case LongType =>
          new FieldType(column.nullable, new ArrowType.Int(64, true), null)
        case FloatType =>
          new FieldType(column.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null)
        case DoubleType =>
          new FieldType(column.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null)
        case StringType =>
          new FieldType(column.nullable, ArrowType.Utf8.INSTANCE, null)
        case BooleanType =>
          new FieldType(column.nullable, ArrowType.Bool.INSTANCE, null)
        case BinaryType =>
          new FieldType(column.nullable, new ArrowType.Binary(), null)
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported type: ${column.colType}")
      }

      new Field(column.name, arrowFieldType, Collections.emptyList())
    }.toList

    new Schema(fields.asJava)
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

  def listFilesWithAttributes(directoryPath: String): Seq[(File, BasicFileAttributes)] = {
    val dir = new File(directoryPath)
    if (dir.exists() && dir.isDirectory) {
      dir.listFiles()
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
      case "bin"  => "Binary File"
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

  def closeFileSource(filePath: String): Unit = {
    resourceManager.close(filePath)
  }

  def closeAllFileSources(): Unit = {
    resourceManager.closeAll()
  }


  def createFileChunkBatch( chunks: Iterator[(Int, String, Array[Byte])],arrowRoot: VectorSchemaRoot, batchSize: Int = 10
                          ): Iterator[ArrowRecordBatch] = {


    val idVec = arrowRoot.getVector("id").asInstanceOf[IntVector]
    val nameVec = arrowRoot.getVector("name").asInstanceOf[VarCharVector]
    //    val indexVec = arrowRoot.getVector("chunkIndex").asInstanceOf[IntVector]
    val contentVec = arrowRoot.getVector("bin").asInstanceOf[VarBinaryVector]
    chunks.grouped(batchSize).map { chunkGroup =>
      arrowRoot.allocateNew()
      chunkGroup.zipWithIndex.foreach { case ((index, filename, chunkData), cnt) =>
        idVec.setSafe(cnt, index) // 当前批次内的序号
        nameVec.setSafe(cnt, filename.getBytes())
        contentVec.setSafe(cnt, chunkData)
      }
      arrowRoot.setRowCount(chunkGroup.size)
      val unloader = new VectorUnloader(arrowRoot)
      unloader.getRecordBatch
    }
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

  //结构化文件分批传输
  def createFileBatch(arrowRoot: VectorSchemaRoot, seq: Seq[String]): ArrowRecordBatch = {
    arrowRoot.allocateNew()

    val vec = arrowRoot.getVector("name").asInstanceOf[VarBinaryVector]

    var i = 0
    seq.foreach(ss => {
      vec.setSafe(i, ss.getBytes("UTF-8"))
      i += 1
    })

    arrowRoot.setRowCount(i)
    val unloader = new VectorUnloader(arrowRoot)
    unloader.getRecordBatch
  }
}
