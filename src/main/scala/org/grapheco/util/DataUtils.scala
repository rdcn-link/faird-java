package org.grapheco.util

import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType}

import scala.jdk.CollectionConverters._
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.spark.SparkException
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types._

import java.io.{File, FileInputStream}
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
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

  def toArrowType(
                   dt: DataType, timeZoneId: String, largeVarTypes: Boolean = false): ArrowType = dt match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(8 * 2, true)
    case IntegerType => new ArrowType.Int(8 * 4, true)
    case LongType => new ArrowType.Int(8 * 8, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case StringType if !largeVarTypes => ArrowType.Utf8.INSTANCE
    case BinaryType if !largeVarTypes => ArrowType.Binary.INSTANCE
    case StringType if largeVarTypes => ArrowType.LargeUtf8.INSTANCE
    case BinaryType if largeVarTypes => ArrowType.LargeBinary.INSTANCE
//    case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale)
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case TimestampType if timeZoneId == null =>
      throw new IllegalStateException("Missing timezoneId where it is mandatory.")
    case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
    case TimestampNTZType =>
      new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
    case NullType => ArrowType.Null.INSTANCE
    case _: YearMonthIntervalType => new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
    case _: DayTimeIntervalType => new ArrowType.Duration(TimeUnit.MICROSECOND)
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported type: ${dt}")
  }



  def toArrowField(
                    name: String,
                    dt: DataType,
                    nullable: Boolean,
                    timeZoneId: String,
                    largeVarTypes: Boolean = false): Field = {
    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null)
        new Field(name, fieldType,
          Seq(toArrowField("element", elementType, containsNull, timeZoneId,
            largeVarTypes)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null)
        new Field(name, fieldType,
          fields.map { field =>
            toArrowField(field.name, field.dataType, field.nullable, timeZoneId, largeVarTypes)
          }.toSeq.asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(name, mapType,
          Seq(toArrowField(MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, keyType, nullable = false)
              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
            nullable = false,
            timeZoneId,
            largeVarTypes)).asJava)
      case udt: UserDefinedType[_] =>
        toArrowField(name, udt.sqlType, nullable, timeZoneId, largeVarTypes)
      case dataType =>
        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId,
          largeVarTypes), null)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  def toArrowSchema(
                     schema: StructType,
                     timeZoneId: String,
                     errorOnDuplicatedFieldNames: Boolean,
                     largeVarTypes: Boolean = false): Schema = {
    new Schema(schema.map { field =>
      toArrowField(
        field.name,
        deduplicateFieldNames(field.dataType, errorOnDuplicatedFieldNames),
        field.nullable,
        timeZoneId,
        largeVarTypes)
    }.asJava)
  }

  def deduplicateFieldNames(
                             dt: DataType, errorOnDuplicatedFieldNames: Boolean): DataType = dt match {
    case udt: UserDefinedType[_] => deduplicateFieldNames(udt.sqlType, errorOnDuplicatedFieldNames)
    case st @ StructType(fields) =>
      val newNames = if (st.names.toSet.size == st.names.length) {
        st.names
      } else {
        if (errorOnDuplicatedFieldNames) {
          throw new UnsupportedOperationException(s"Unsupported type: ${st.names}")
        }
        val genNawName = st.names.groupBy(identity).map {
          case (name, names) if names.length > 1 =>
            val i = new AtomicInteger()
            name -> { () => s"${name}_${i.getAndIncrement()}" }
          case (name, _) => name -> { () => name }
        }
        st.names.map(genNawName(_)())
      }
      val newFields =
        fields.zip(newNames).map { case (StructField(_, dataType, nullable, metadata), name) =>
          StructField(
            name, deduplicateFieldNames(dataType, errorOnDuplicatedFieldNames), nullable, metadata)
        }
      StructType(newFields)
    case ArrayType(elementType, containsNull) =>
      ArrayType(deduplicateFieldNames(elementType, errorOnDuplicatedFieldNames), containsNull)
    case MapType(keyType, valueType, valueContainsNull) =>
      MapType(
        deduplicateFieldNames(keyType, errorOnDuplicatedFieldNames),
        deduplicateFieldNames(valueType, errorOnDuplicatedFieldNames),
        valueContainsNull)
    case _ => dt
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
