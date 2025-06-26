package link.rdcn.client

import link.rdcn.SimpleSerializer
import link.rdcn.struct.Row
import link.rdcn.user.Credentials
import org.apache.arrow.flight.{AsyncPutListener, FlightClient, FlightDescriptor, FlightInfo, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import java.util.UUID
import java.io.ByteArrayOutputStream
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 14:45
 * @Modified By:
 */

trait ProtocolClient{
  def listDataSetNames(): Seq[String]
  def listDataFrameNames(dsName: String): Seq[String]
  def getDataSetMetaData(dsName: String): String
  def close(): Unit
  def getRows(dataFrameName: String, ops: List[DFOperation]): Iterator[Row]
}
class ArrowFlightClient(url: String, port:Int) extends ProtocolClient{

  val location = Location.forGrpcInsecure(url, port)
  val allocator: BufferAllocator = new RootAllocator()
  private val flightClient = FlightClient.builder(allocator, location).build()
  private val userToken = UUID.randomUUID().toString

  def login(credentials: Credentials): Unit = {
    val paramFields: Seq[Field] = List(
      new Field("credentials", FieldType.nullable(new ArrowType.Binary()), null),
    )
    val schema = new Schema(paramFields.asJava)
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val credentialsVector = vectorSchemaRoot.getVector("credentials").asInstanceOf[VarBinaryVector]
    credentialsVector.allocateNew(1)
    credentialsVector.set(0, SimpleSerializer.serialize(credentials))
    vectorSchemaRoot.setRowCount(1)
    val listener = flightClient.startPut(FlightDescriptor.path(s"login.$userToken"), vectorSchemaRoot, new AsyncPutListener())
    listener.putNext()
    listener.completed()
    listener.getResult()
  }

  def listDataSetNames(): Seq[String] = {
    val flightInfo = flightClient.getInfo(FlightDescriptor.path("listDataSetNames"))
    getListStrByFlightInfo(flightInfo)
  }
  def listDataFrameNames(dsName: String): Seq[String] = {
    val flightInfo = flightClient.getInfo(FlightDescriptor.path(s"listDataFrameNames.$dsName"))
    getListStrByFlightInfo(flightInfo)
  }

  def getSchema(dataFrameName: String): String = {
    val flightInfo = flightClient.getInfo(FlightDescriptor.path(s"getSchema.$dataFrameName"))
    getStrByFlightInfo(flightInfo)
  }

  override def getDataSetMetaData(dataSetName: String): String = {
    val flightInfo = flightClient.getInfo(FlightDescriptor.path(s"getDataSetMetaData.$dataSetName"))
    getStrByFlightInfo(flightInfo)
  }

  def getSchemaURI(dataFrameName: String): String = {
    val flightInfo = flightClient.getInfo(FlightDescriptor.path(s"getSchemaURI.$dataFrameName"))
    getStrByFlightInfo(flightInfo)
  }

  def close(): Unit = {
    flightClient.close()
  }

  def getRows(dataFrameName: String, ops: List[DFOperation]): Iterator[Row]  = {
    //上传参数
    val paramFields: Seq[Field] = List(

      new Field("dfName", FieldType.nullable(new ArrowType.Utf8()), null),
      new Field("userToken", FieldType.nullable(new ArrowType.Utf8()), null),
      new Field("DFOperation", FieldType.nullable(new ArrowType.Binary()), null)
    )
    val schema = new Schema(paramFields.asJava)
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val dfNameCharVector = vectorSchemaRoot.getVector("dfName").asInstanceOf[VarCharVector]
    val tokenCharVector = vectorSchemaRoot.getVector("userToken").asInstanceOf[VarCharVector]
    val DFOperationVector = vectorSchemaRoot.getVector("DFOperation").asInstanceOf[VarBinaryVector]
    dfNameCharVector.allocateNew(1)
    dfNameCharVector.set(0, dataFrameName.getBytes("UTF-8"))
    tokenCharVector.allocateNew(1)
    tokenCharVector.set(0, userToken.getBytes("UTF-8"))
    if(ops.length == 0){
      DFOperationVector.allocateNew(1)
      vectorSchemaRoot.setRowCount(1)
    }else{
      DFOperationVector.allocateNew(ops.length)
      for(i <- 0 to ops.length -1){
        DFOperationVector.set(i, SimpleSerializer.serialize(ops(i)))
      }
      vectorSchemaRoot.setRowCount(ops.length)
    }

    val requestSchemaId = UUID.randomUUID().toString
    val listener = flightClient.startPut(FlightDescriptor.path(requestSchemaId), vectorSchemaRoot, new AsyncPutListener())
    listener.putNext()
    listener.completed()
    listener.getResult()
    //获取数据
    val flightInfo = flightClient.getInfo(FlightDescriptor.path(requestSchemaId))
    //flightInfo 中可以获取schema
    println(s"Client (Get Metadata): $flightInfo")
    val flightInfoSchema = flightInfo.getSchema
    val isBinaryColumn = if (flightInfoSchema.getFields.size() <= 7) false
    else flightInfoSchema.getFields.get(7).getType match {
      case _: ArrowType.Binary => true
      case _ => false
    }
    val flightStream = flightClient.getStream(flightInfo.getEndpoints.get(0).getTicket)
    val iter: Iterator[Seq[Seq[Any]]] = new Iterator[Seq[Seq[Any]]] {
      override def hasNext: Boolean = flightStream.next()


      override def next(): Seq[Seq[Any]] = {
        val vectorSchemaRootReceived = flightStream.getRoot
        val rowCount = vectorSchemaRootReceived.getRowCount
        val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
        //        var it = Seq.range(0, rowCount).toIterator
        Seq.range(0, rowCount).map(index => {
          val rowMap = fieldVectors.map(vec => {
            if (vec.isNull(index)) (vec.getName, null)
            else vec match {
              case v: org.apache.arrow.vector.IntVector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.VarCharVector => (vec.getName, new String(v.get(index)))
              case v: org.apache.arrow.vector.Float8Vector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.BitVector => (vec.getName, v.get(index) == 1)
              case v: org.apache.arrow.vector.VarBinaryVector => (vec.getName, v.get(index))
              case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getClass}")
            }
          }).toMap
          val r: Seq[Any] = rowMap.toSeq.map(x => x._2)
          //                    Row(rowMap.toSeq.map(x => x._2): _*)
          r
        })
      }
    }
    val flatIter: Iterator[Seq[Any]] = iter.flatMap(rows => rows)

    if (!isBinaryColumn) {
      // 第三列不是binary类型，直接返回Row(Seq[Any])
      flatIter.map(seq => Row.fromSeq(seq))
    } else {

      var isFirstChunk: Boolean = true
      var currentSeq: Seq[Any] = flatIter.next()
      var cachedSeq: Seq[Any] = currentSeq
      var currentChunk: Array[Byte] = Array[Byte]()
      var cachedChunk: Array[Byte] = currentSeq(4).asInstanceOf[Array[Byte]]
      var cachedName: String = currentSeq(0).asInstanceOf[String]
      var currentName: String = currentSeq(0).asInstanceOf[String]
      new Iterator[Row] {
        override def hasNext: Boolean = flatIter.hasNext || cachedChunk.nonEmpty

        override def next(): Row = {

          val blobIter: Iterator[Array[Byte]] = new Iterator[Array[Byte]] {

            private var isExhausted: Boolean = false // flatIter 是否耗尽

            // 预读取下一块的 index 和 struct（如果存在）
            private def readNextChunk(): Unit = {
              if (flatIter.hasNext) {
                if (!isFirstChunk) {
                  val nextSeq: Seq[Any] = flatIter.next()
                  val nextName: String = nextSeq(0).asInstanceOf[String]
                  val nextChunk: Array[Byte] = nextSeq(4).asInstanceOf[Array[Byte]]
                  if (nextName != currentName) {
                    // index 变化，结束当前块
                    isExhausted = true
                    isFirstChunk = true
                    cachedSeq = nextSeq
                    cachedName = nextName
                    cachedChunk = nextChunk
                  } else {

                    currentChunk = nextChunk
                    currentName = nextName
                  }
                  currentSeq = nextSeq
                  currentName = nextName
                } else {
                  currentSeq = cachedSeq
                  currentName = cachedName
                  currentChunk = cachedChunk
                  isExhausted = false
                  isFirstChunk = false
                }
                //              println(currentIndex)

              } else {
                // flatIter 耗尽
                if (cachedChunk.nonEmpty)
                  currentChunk = cachedChunk
                isExhausted = true
              }
            }

            // hasNext: 检查是否还有块（可能预读取）
            override def hasNext: Boolean = {
              if (currentChunk.isEmpty && !isExhausted) {
                readNextChunk() // 如果当前块为空且迭代器未结束，尝试预读取
              }
              !isExhausted || currentChunk.nonEmpty
            }

            // next: 返回当前块（已由 hasNext 预加载）
            override def next(): Array[Byte] = {

              if (!isFirstChunk) {
                val chunk = currentChunk
                currentChunk = Array.empty[Byte]
                if (!flatIter.hasNext)
                  cachedChunk = Array.empty[Byte]
                chunk
                // 手动清空
              } else {
                val chunk = cachedChunk
                cachedChunk = Array.empty[Byte]
                currentChunk = Array.empty[Byte]
                chunk
              }

            }
          }
          Row(currentSeq.patch(4, Nil, 1):+new Blob(blobIter):_*)
          //          Row(iter.next())
        }
      }
    }
  }

  private def getListStrByFlightInfo(flightInfo: FlightInfo): Seq[String] = {
    val flightStream = flightClient.getStream(flightInfo.getEndpoints.get(0).getTicket)
    if(flightStream.next()){
      val vectorSchemaRootReceived = flightStream.getRoot
      val rowCount = vectorSchemaRootReceived.getRowCount
      val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
      Seq.range(0, rowCount).map(index  => {
        val rowMap = fieldVectors.map(vec =>{
          vec.asInstanceOf[VarCharVector].getObject(index).toString
        }).head
        rowMap
      })
    }else Seq.empty
  }

  private def getStrByFlightInfo(flightInfo: FlightInfo): String = {
      val flightStream = flightClient.getStream(flightInfo.getEndpoints.get(0).getTicket)
      if(flightStream.next()){
        val vectorSchemaRootReceived = flightStream.getRoot
        val rowCount = vectorSchemaRootReceived.getRowCount
        val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
        fieldVectors.head.asInstanceOf[VarCharVector].getObject(0).toString
      }else ""
    }

}

// 表示完整的二进制文件
class Blob( val chunkIterator:Iterator[Array[Byte]]) extends Serializable {
  // 缓存加载后的完整数据
  private var _content: Option[Array[Byte]] = None
  // 缓存文件大小（独立于_content，避免获取大数组长度）
  private var _size: Option[Long] = None

  private var _chunkCount: Option[Int] = None

  private var _memoryReleased: Boolean = false

  private def loadLazily(): Unit = {
//    println("loadLazily")
    if (_content.isEmpty && _size.isEmpty && _chunkCount.isEmpty) {
      val byteStream = new ByteArrayOutputStream()
      var totalSize: Long = 0L
      var chunkCount = 0

      while (chunkIterator.hasNext) {
        val chunk = chunkIterator.next()
//        println(chunk.mkString("Array(", ", ", ")"))
        totalSize += chunk.length
        chunkCount+=1
        byteStream.write(chunk)
        byteStream.reset()

      }
//      println("loaded Lazily")
      _content = Some(byteStream.toByteArray)
      _size = Some(totalSize)
      _chunkCount = Some(chunkCount)
      byteStream.close()
    }
  }


  /** 获取完整的文件内容 */
  def content: Array[Byte] = {
    if (_memoryReleased) {
      throw new IllegalStateException("Blob content memory has been released")
    }
    if (_content.isEmpty) loadLazily()
    _content.get
  }


  /** 获取文件大小 */
  def size: Long = {
    if (_size.isEmpty) loadLazily()
    _size.get
  }

  /** 获取分块数量 */
  def chunkCount: Int = {
    if (_chunkCount.isEmpty) loadLazily()
    _chunkCount.get
  }

  /** 释放content占用的内存 */
  def releaseContentMemory(): Unit = {
    _content = None
    _memoryReleased = true
    System.gc()
  }

  /** 获取分块迭代器 */
//  def chunkIterator: Iterator[Array[Byte]] = chunkIterator

  override def toString: String = s"Blob()"
}