package org.grapheco.util

import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types.{BinaryType, BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType}

import java.io.{File, FileInputStream}
import java.util.Collections
import scala.io.Source
import scala.jdk.CollectionConverters.seqAsJavaListConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/19 16:03
 * @Modified By:
 */
object DataUtils {
  //建议使用org.apache.spark.sql.util.ArrowUtils.toArrowSchema(Schema, "UTC")
  def sparkSchemaToArrowSchema(sparkSchema: StructType): Schema = {
    val fields: List[Field] = sparkSchema.fields.map { field =>
      val arrowFieldType = field.dataType match {
        case IntegerType =>
          new FieldType(field.nullable, new ArrowType.Int(32, true), null)
        case LongType =>
          new FieldType(field.nullable, new ArrowType.Int(64, true), null)
        case FloatType =>
          new FieldType(field.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null)
        case DoubleType =>
          new FieldType(field.nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null)
        case StringType =>
          new FieldType(field.nullable, ArrowType.Utf8.INSTANCE, null)
        case BooleanType =>
          new FieldType(field.nullable, ArrowType.Bool.INSTANCE, null)
        case BinaryType =>
          new FieldType(field.nullable, new ArrowType.Binary(), null)
        case _ =>
          throw new UnsupportedOperationException(s"Unsupported type: ${field.dataType}")
      }

      new Field(field.name, arrowFieldType, Collections.emptyList())
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
    source.getLines()
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

    new Iterator[Array[Byte]] {
      override def hasNext: Boolean = inputStream.available() > 0

      override def next(): Array[Byte] = {
        val bufferSize = Math.min(chunkSize, inputStream.available())
        val buffer = new Array[Byte](bufferSize)
        val bytesRead = inputStream.read(buffer)
        if (bytesRead == -1) {
          inputStream.close()
          Iterator.empty.next()
        } else if (bytesRead < buffer.length) {
          inputStream.close()
          buffer.take(bytesRead)
        } else {
          buffer
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
