package org.grapheco.server

import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.grapheco.SimpleSerializer
import org.grapheco.client.DFOperation

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

object FairdServer extends App {
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

class FlightProducerImpl(allocator: BufferAllocator, location: Location) extends NoOpFlightProducer {

  private val requestMap = new ConcurrentHashMap[FlightDescriptor, RemoteDataFrameImpl]()
  private val batchLen = 1000
  private val numOfBatch = 50000
  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {

    new Runnable {
      override def run(): Unit = {
        while (flightStream.next()){
          val root = flightStream.getRoot
          val rowCount = root.getRowCount
          val source = root.getFieldVectors.get(0).asInstanceOf[VarCharVector].getObject(0).toString
          val dfOperations: List[DFOperation] = List.range(0, rowCount).map(index => {
            val bytes = root.getFieldVectors.get(1).asInstanceOf[VarBinaryVector].get(index)
            if(bytes==null) null else
              SimpleSerializer.deserialize(bytes).asInstanceOf[DFOperation]
          })
          val remoteDataFrameImpl = if(dfOperations.contains(null)) RemoteDataFrameImpl(source = source, List.empty)
          else RemoteDataFrameImpl(source = source, ops = dfOperations)
          requestMap.put(flightStream.getDescriptor, remoteDataFrameImpl)
          flightStream.getRoot.clear()
        }
        ackStream.onCompleted()
      }
    }
  }

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))
    val request: RemoteDataFrameImpl = requestMap.get(flightDescriptor)

//    val fields: List[Field] = List(new Field("name", FieldType.nullable(new ArrowType.Binary), null))
      val fields: List[Field] = List(
        new Field("id", FieldType.nullable(new ArrowType.Int(32,true)), null),
        new Field("name", FieldType.nullable(new ArrowType.Binary()), null)
      )

    val schema = new Schema(fields.asJava)

    val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(schema, childAllocator)
    try{
      val loader = new VectorLoader(root)
      listener.start(root)
      //每1000条row为一批进行传输,将DataFrame转化成Iterator，不会一次性加载到内存

      //      读取文件为 stream 流传输数据
//      groupedLines("C:\\Users\\Yomi\\Downloads\\数据\\1.csv", 1000).foreach(lines => {
//        val batch = createFileBatch(root, lines)
//        try {
//          loader.load(batch)
//          while (!listener.isReady()) {
////            Thread.onSpinWait()
//            LockSupport.parkNanos(1)
//          }
//          listener.putNext()
//        }finally {
//          batch.close()
//        }
//      })


              val batch = createFileChunkBatch(root)
              try {
                loader.load(batch)
                while (!listener.isReady()) {
//                  Thread.onSpinWait()
                  LockSupport.parkNanos(1)
                }
                listener.putNext()
              }finally {
                batch.close()
              }


      listener.completed()
    } catch {
      case e: Throwable => listener.error(e)
        throw e
    }finally {
      if (root != null) root.close()
      if (childAllocator != null) childAllocator.close()
      requestMap.remove(flightDescriptor)
    }
  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
    new FlightInfo(new Schema(List.empty.asJava), descriptor, List(flightEndpoint).asJava, -1L, 0L)
  }

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
    requestMap.forEach{
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
    for (i <- 0 to (rowCount -1)){
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

  private def createFileChunkBatch(arrowRoot: VectorSchemaRoot): ArrowRecordBatch = {
    arrowRoot.allocateNew()
    var index = 0
    val idVec = arrowRoot.getVector("id").asInstanceOf[IntVector]
    val contentVec = arrowRoot.getVector("name").asInstanceOf[VarBinaryVector]
    for (i <- 0 to 5-1){
      readFileInChunks("C:\\Users\\Yomi\\Downloads\\数据\\1.csv").foreach(bytes => {
        idVec.setSafe(index,i)
        contentVec.setSafe(index, bytes)
        index += 1
      })
    }
    arrowRoot.setRowCount(index)
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

  import java.io.{File, FileInputStream}

  private def readFileInChunks(filePath: String, chunkSize: Int = 5 * 1024 * 1024): Iterator[Array[Byte]] = {
    val file = new File(filePath)
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
}
