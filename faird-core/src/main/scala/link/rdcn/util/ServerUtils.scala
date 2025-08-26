package link.rdcn.util

import com.sun.management.OperatingSystemMXBean
import link.rdcn.struct.{DataFrame, StructType, ValueType}
import link.rdcn.struct.ValueType.{BinaryType, BooleanType, DoubleType, FloatType, IntType, LongType, StringType}
import org.apache.arrow.flight.{FlightProducer, Result}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector._
import org.apache.jena.rdf.model.Model

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.management.ManagementFactory
import java.util.Collections
import scala.collection.JavaConverters.{asScalaIteratorConverter, seqAsJavaListConverter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/19 16:03
 * @Modified By:
 */
object ServerUtils {

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


  def getBytesFromVectorSchemaRoot(root: VectorSchemaRoot): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, null, outputStream)
    writer.start()
    writer.writeBatch()
    writer.end()
    writer.close()
    outputStream.toByteArray
  }

  def getVectorSchemaRootFromBytes(bytes: Array[Byte], allocator: BufferAllocator): VectorSchemaRoot = {
    val inputStream = new ByteArrayInputStream(bytes)
    val reader = new ArrowStreamReader(inputStream, allocator)
    reader.loadNextBatch()
    reader.getVectorSchemaRoot
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

  def getSingleLongBytesStream(long: Long, listener: FlightProducer.StreamListener[Result]): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("rowCount", ValueType.LongType))
    try {
      val nameVector = rootAndAllocator._1.getVector("rowCount").asInstanceOf[BigIntVector]
      rootAndAllocator._1.allocateNew()
      nameVector.setSafe(0, long)
      rootAndAllocator._1.setRowCount(1)
      listener.onNext(new Result(ServerUtils.getBytesFromVectorSchemaRoot(rootAndAllocator._1)))
      listener.onCompleted()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }

  def getListStringStream(seq: Seq[String], listener: FlightProducer.StreamListener[Result]): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("name", ValueType.StringType))
    try {
      val nameVector = rootAndAllocator._1.getVector("name").asInstanceOf[VarCharVector]
      rootAndAllocator._1.allocateNew()
      var index = 0
      seq.foreach(d => {
        nameVector.setSafe(index, d.getBytes("UTF-8"))
        index += 1
      })
      rootAndAllocator._1.setRowCount(index)
      listener.onNext(new Result(ServerUtils.getBytesFromVectorSchemaRoot(rootAndAllocator._1)))
      listener.onCompleted()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }
  def sendDataFrame(df: DataFrame, listener: FlightProducer.StreamListener[Result]): Unit = {
    // 1. 根据 DataFrame 的 schema 构建 Arrow VectorSchemaRoot
    val structType = df.schema
    val rootAndAllocator = getRootByStructType(structType)  // 返回 (VectorSchemaRoot, BufferAllocator)
    val root = rootAndAllocator._1
    val allocator = rootAndAllocator._2

    try {
      root.allocateNew() // 分配内存

      var rowIndex = 0
      df.mapIterator(stream => {
        stream.foreach { row =>
          structType.columns.zipWithIndex.foreach { case (col, colIndex) =>
            val vector = root.getVector(col.name)
            col.colType match {
              case ValueType.StringType =>
                vector.asInstanceOf[VarCharVector].setSafe(rowIndex, row.get(colIndex).toString.getBytes("UTF-8"))
              case ValueType.IntType =>
                vector.asInstanceOf[IntVector].setSafe(rowIndex, row.get(colIndex).asInstanceOf[Int])
              case ValueType.LongType =>
                vector.asInstanceOf[BigIntVector].setSafe(rowIndex, row.get(colIndex).asInstanceOf[Long])
              case ValueType.FloatType =>
                vector.asInstanceOf[Float4Vector].setSafe(rowIndex, row.get(colIndex).asInstanceOf[Float])
              case ValueType.DoubleType =>
                vector.asInstanceOf[Float8Vector].setSafe(rowIndex, row.get(colIndex).asInstanceOf[Double])
              case ValueType.BooleanType =>
                vector.asInstanceOf[BitVector].setSafe(rowIndex, if (row.get(colIndex).asInstanceOf[Boolean]) 1 else 0)
              case ValueType.BinaryType =>
                vector.asInstanceOf[VarBinaryVector].setSafe(rowIndex, row.get(colIndex).asInstanceOf[Array[Byte]])
              case _ =>
                throw new UnsupportedOperationException(s"Unsupported type: ${col.colType}")
            }
          }
          rowIndex += 1
        }
      })
      root.setRowCount(rowIndex)

      listener.onNext(new Result(ServerUtils.getBytesFromVectorSchemaRoot(root)))
      listener.onCompleted()

    } finally {
      root.close()
      allocator.close()
    }
  }

  def getSingleStringStream(str: String, listener: FlightProducer.StreamListener[Result]): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("name", ValueType.StringType))
    try {
      val nameVector = rootAndAllocator._1.getVector("name").asInstanceOf[VarCharVector]
      rootAndAllocator._1.allocateNew()
      nameVector.setSafe(0, str.getBytes("UTF-8"))
      rootAndAllocator._1.setRowCount(1)
      listener.onNext(new Result(ServerUtils.getBytesFromVectorSchemaRoot(rootAndAllocator._1)))
      listener.onCompleted()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }

  def getArrayBytesStream(bytes: Array[Byte], listener: FlightProducer.StreamListener[Result]): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("name", ValueType.BinaryType))
    try {
      val nameVector = rootAndAllocator._1.getVector("name").asInstanceOf[VarBinaryVector]
      rootAndAllocator._1.allocateNew()
      nameVector.setSafe(0, bytes)
      rootAndAllocator._1.setRowCount(1)
      listener.onNext(new Result(ServerUtils.getBytesFromVectorSchemaRoot(rootAndAllocator._1)))
      listener.onCompleted()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }

  def getRootByStructType(structType: StructType): (VectorSchemaRoot, BufferAllocator) = {
    val schema = convertStructTypeToArrowSchema(structType)
    val childAllocator: BufferAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(schema, childAllocator)
    (root, childAllocator)
  }

  def getResourceStatusString(): Map[String, String] = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[OperatingSystemMXBean]
    val runtime = Runtime.getRuntime

    val cpuLoadPercent = (osBean.getSystemCpuLoad * 100).formatted("%.2f")
    val availableProcessors = osBean.getAvailableProcessors

    val totalMemory = runtime.totalMemory() / 1024 / 1024 // MB
    val freeMemory = runtime.freeMemory() / 1024 / 1024   // MB
    val maxMemory = runtime.maxMemory() / 1024 / 1024     // MB
    val usedMemory = totalMemory - freeMemory

    val systemMemoryTotal = osBean.getTotalPhysicalMemorySize / 1024 / 1024 // MB
    val systemMemoryFree = osBean.getFreePhysicalMemorySize / 1024 / 1024   // MB
    val systemMemoryUsed = systemMemoryTotal - systemMemoryFree
    Map(
      "cpu.cores" -> s"$availableProcessors",
      "cpu.usage.percent" -> s"$cpuLoadPercent%",
      "jvm.memory.max.mb" -> s"$maxMemory MB",
      "jvm.memory.total.mb" -> s"$totalMemory MB",
      "jvm.memory.used.mb" -> s"$usedMemory MB",
      "jvm.memory.free.mb" -> s"$freeMemory MB",
      "system.memory.total.mb" -> s"$systemMemoryTotal MB",
      "system.memory.used.mb" -> s"$systemMemoryUsed MB",
      "system.memory.free.mb" -> s"$systemMemoryFree MB"
    )
  }

  def modelToMap(model: Model): Map[String, Map[String, List[String]]] = {
    val stmts = model.listStatements().asScala.toList

    stmts
      .groupBy(_.getSubject.toString) // 按 subject 分组
      .map { case (subject, statements) =>
        val predObjMap = statements
          .groupBy(_.getPredicate.toString) // 按 predicate 分组
          .map { case (predicate, stmtsForPred) =>
            val objs = stmtsForPred.map(_.getObject.toString)
            predicate -> objs
          }
        subject -> predObjMap
      }
  }

  def init(allocatorServer: BufferAllocator): Unit = {
    allocator = allocatorServer
  }

  private var allocator: BufferAllocator = _
}
