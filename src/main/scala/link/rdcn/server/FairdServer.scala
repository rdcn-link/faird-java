package link.rdcn.server

import link.rdcn.client.{DFOperation, DataAccessRequest, RemoteDataFrameImpl}
import link.rdcn.util.DataUtils
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.types.{BinaryType, BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType}
import DataUtils.sparkSchemaToArrowSchema
import link.rdcn.{Logging, SimpleSerializer}
import link.rdcn.provider.{DataFrameSource, DataFrameSourceFactoryImpl, DynamicDataFrameSourceFactory, MockDataFrameProvider}

import java.io.{File, FileInputStream, IOException}
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters._
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import scala.collection.mutable.ArrayBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 18:36
 * @Modified By:
 */

object FairdServer extends App with Logging {
  val location = Location.forGrpcInsecure("0.0.0.0", 33333)
  val allocator: BufferAllocator = new RootAllocator()

  try {
    val producer = new FlightProducerImpl(allocator, location)
    val flightServer = FlightServer.builder(allocator, location, producer).build()

    flightServer.start()
    println(s"Server (Location): Listening on port ${flightServer.getPort}")
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        flightServer.close()
        producer.close()
      }
    })
    flightServer.awaitTermination()
  } catch {
    case e: Exception => e.printStackTrace()
  }
}

class FlightProducerImpl(allocator: BufferAllocator, location: Location, provider: MockDataFrameProvider = null) extends NoOpFlightProducer with Logging {

  private val requestMap = new ConcurrentHashMap[FlightDescriptor, RemoteDataFrameImpl]()
  private val batchLen = 1000
  private val numOfBatch = 50000

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {

    new Runnable {
      override def run(): Unit = {
        while (flightStream.next()) {
          val root = flightStream.getRoot
          val rowCount = root.getRowCount
          val sourceBytes = root.getFieldVectors.get(0).asInstanceOf[VarBinaryVector].getObject(0)
          val source = SimpleSerializer.deserialize(sourceBytes).asInstanceOf[DataAccessRequest]
          val dfOperations: List[DFOperation] = List.range(0, rowCount).map(index => {
            val bytes = root.getFieldVectors.get(1).asInstanceOf[VarBinaryVector].get(index)
            if (bytes == null) null else
              SimpleSerializer.deserialize(bytes).asInstanceOf[DFOperation]
          })
          val remoteDataFrameImpl = if (dfOperations.contains(null)) RemoteDataFrameImpl(source = source, List.empty)
          else RemoteDataFrameImpl(source = source, ops = dfOperations)
          requestMap.put(flightStream.getDescriptor, remoteDataFrameImpl)
          flightStream.getRoot.clear()
        }
        ackStream.onCompleted()
      }
    }
  }

  private def getListStrStream(seq: Seq[String], listener: FlightProducer.ServerStreamListener): Unit = {
    val fields: List[Field] = List(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null))
    val schema = new Schema(fields.asJava)
    val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(schema, childAllocator)
    try {
      val nameVector = root.getVector("name").asInstanceOf[VarCharVector]
      root.allocateNew()
      var index = 0
      seq.foreach(d => {
        nameVector.setSafe(index, d.getBytes("UTF-8"))
        index += 1
      })
      root.setRowCount(index)
      listener.start(root)
      listener.putNext()
      listener.completed()
    } finally {
      root.close()
      childAllocator.close()
    }
  }

  private def getStrStream(str: String, listener: FlightProducer.ServerStreamListener): Unit = {
    val fields: List[Field] = List(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null))
    val schema = new Schema(fields.asJava)
    val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(schema, childAllocator)
    try {
      val nameVector = root.getVector("name").asInstanceOf[VarCharVector]
      root.allocateNew()
      var index = 0
      nameVector.setSafe(index, str.getBytes("UTF-8"))
      index += 1
      root.setRowCount(index)
      listener.start(root)
      listener.putNext()
      listener.completed()
    } finally {
      root.close()
      childAllocator.close()
    }
  }

  def listFiles(dirPath: String): List[String] = {
    val dir = new File(dirPath)
    if (dir.exists && dir.isDirectory) {
      dir.listFiles
        .map(_.getName)
        .filter(x => x != ".DS_Store")
        .toList
    } else {
      List.empty
    }
  }

  //读取配置文件，通过DataFrameProvider提供
  private val datasets = listFiles("/Users/renhao/Downloads/MockData")
  private val dataFrames: Map[String, List[String]] = datasets.map(x => (x, listFiles("/Users/renhao/Downloads/MockData/" + x))).toMap
  private val dfName2dF: Map[String, String] = Map.empty


  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
        val provider = new MockDataFrameProvider
        val factory = new DynamicDataFrameSourceFactory(provider)
        new String(ticket.getBytes, StandardCharsets.UTF_8) match {
      case "listDataSetNames" => getListStrStream(datasets, listener)
      case ss if ss.startsWith("listDataFrameNames") => {
        getListStrStream(dataFrames.get(ss.split("\\.")(1)).getOrElse(List.empty), listener)
      }
      case ss if ss.startsWith("getSchemaURI") => {
        getStrStream(provider.getSchemaURI(ss.split("\\.")(1)),listener)

      }
      case ss if ss.startsWith("getSchema") => {
        getStrStream(provider.getSchema(ss.split("\\.")(1)).toString,listener)
      }
      case ss if ss.startsWith("getMetaData") => {
        getStrStream(provider.getMetaData(ss.split("\\.")(1)),listener)

      }

      case _ => {

        /** *
         * request 包含
         * 1、user 信息
         * 2、访问 DataFrame位置，DataFrame名称所属DataSet
         * 3、DataFrame的转换操作
         * 4、访问 DataFrame的schema？如果是结构化数据分隔符？
         */
        val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))
        val request: RemoteDataFrameImpl = requestMap.get(flightDescriptor)
        val df: DataFrameSource = provider.getDataFrameSource(request, factory)
        val schema = DataUtils.sparkSchemaToArrowSchema(provider.getSchema(request.source.datasetId))


        //能否支持并发
        val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(schema, childAllocator)
        val loader = new VectorLoader(root)
        listener.start(root)
        try {
          df.getArrowRecordBatch(root).foreach(batch => {
            try {
              loader.load(batch)
              while (!listener.isReady()) {
                Thread.onSpinWait()
              }
              listener.putNext()
            } finally {
              batch.close()
            }
          })
          listener.completed()
        } catch {
          case e: Throwable => listener.error(e)
            e.printStackTrace()
            throw e
        } finally {
          if (root != null) root.close()
          if (childAllocator != null) childAllocator.close()
          requestMap.remove(flightDescriptor)
        }
      }
    }

  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
      // 添加路径有效性检查
      val path = descriptor.getPath
      val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
      val request = requestMap.getOrDefault(descriptor, null)

    //    val schema = if (request != null) sparkSchemaToArrowSchema(request.source.expectedSchema) else new Schema(List.empty.asJava)
      val schema =  if (request != null) sparkSchemaToArrowSchema(provider.getSchema(request.source.datasetId)) else new Schema(List.empty.asJava)
    new FlightInfo(schema, descriptor, List(flightEndpoint).asJava, -1L, 0L)
  }

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
    requestMap.forEach {
      (k, v) => listener.onNext(getFlightInfo(null, k))
    }
    listener.onCompleted()
  }

  def close(): Unit = {

  }

  //内存中生成数据
  private def createCacheBatch(arrowRoot: VectorSchemaRoot): ArrowRecordBatch = {
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

  //结构化文件分批传输
  private def createFileBatch(arrowRoot: VectorSchemaRoot, seq: Seq[String]): ArrowRecordBatch = {
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

  import scala.io.Source

  private def groupedLines(filePath: String, batchSize: Int): Iterator[Seq[String]] = {
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

}
