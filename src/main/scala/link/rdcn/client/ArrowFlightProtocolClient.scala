package link.rdcn.client


import io.circe.{DecodingFailure, parser}
import link.rdcn.SimpleSerializer
import link.rdcn.provider.DataFrameDocument
import link.rdcn.struct.Row
import link.rdcn.user.Credentials
import link.rdcn.util.DataUtils
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{BigIntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileOutputStream, InputStream}
import java.nio.file.Paths
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter, seqAsJavaListConverter}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable


/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 14:45
 * @Modified By:
 */

trait ProtocolClient {
  def listDataSetNames(): Seq[String]

  def listDataFrameNames(dsName: String): Seq[String]

  def getDataSetMetaData(dsName: String): String

  def close(): Unit

  def getRows(dataFrameName: String, ops: String): Iterator[Row]

  def getDataFrameSize(dataFrameName: String): Long

  def getHostInfo(): Map[String, String]

  def getServerResourceInfo(): Map[String, String]
}

class ArrowFlightProtocolClient(url: String, port: Int, useTLS: Boolean = false) extends ProtocolClient {

  val location = {
    if (useTLS) Location.forGrpcTls(url, port) else
      Location.forGrpcInsecure(url, port)
  }
  val allocator: BufferAllocator = new RootAllocator()
  private val flightClient = FlightClient.builder(allocator, location).build()
  private var userToken:Option[String] = None

  def login(credentials: Credentials): Unit = {
    val clientAuthHandler = new FlightClientAuthHandler(credentials)
    flightClient.authenticate(clientAuthHandler)
    userToken = Some(clientAuthHandler.getSessionToken)
    val paramFields: Seq[Field] = List(
      new Field("credentials", FieldType.nullable(new ArrowType.Binary()), null),
    )
    val schema = new Schema(paramFields.asJava)
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val credentialsVector = vectorSchemaRoot.getVector("credentials").asInstanceOf[VarBinaryVector]
    credentialsVector.allocateNew(1)
    credentialsVector.set(0, SimpleSerializer.serialize(credentials))
    vectorSchemaRoot.setRowCount(1)
    val body = DataUtils.getBytesFromVectorSchemaRoot(vectorSchemaRoot)
    val result = flightClient.doAction(new Action(s"login.${userToken.orNull}", body)).asScala
    result.hasNext
  }

  def listDataSetNames(): Seq[String] = {
    val dataSetNames = flightClient.doAction(new Action("listDataSetNames")).asScala
    getListStringByResult(dataSetNames)
  }

  def listDataFrameNames(dsName: String): Seq[String] = {
    val dataFrameNames = flightClient.doAction(new Action(s"listDataFrameNames.$dsName")).asScala
    getListStringByResult(dataFrameNames)
  }

  def getSchema(dataFrameName: String): String = {
    val schema = flightClient.doAction(new Action(s"getSchema.$dataFrameName")).asScala
    getSingleStringByResult(schema)
  }

  override def getDataSetMetaData(dataSetName: String): String = {
    val dataSetMetaData = flightClient.doAction(new Action(s"getDataSetMetaData.$dataSetName")).asScala
    getSingleStringByResult(dataSetMetaData)
  }

  override def getDataFrameSize(dataFrameName: String): Long = {
    val dataFrameSize = flightClient.doAction(new Action(s"getDataFrameSize.$dataFrameName")).asScala
    getSingleLongByResult(dataFrameSize)
  }

  override def getHostInfo(): Map[String, String] = {
    val hostInfo = flightClient.doAction(new Action(s"getHostInfo")).asScala
    getMapByJsonString(getSingleStringByResult(hostInfo))
  }

  override def getServerResourceInfo(): Map[String, String] = {
    val serverResourceInfo = flightClient.doAction(new Action(s"getServerResourceInfo")).asScala
    getMapByJsonString(getSingleStringByResult(serverResourceInfo))
  }

  def getSchemaURI(dataFrameName: String): String = {
    val schemaURI = flightClient.doAction(new Action(s"getSchemaURI.$dataFrameName")).asScala
    getSingleStringByResult(schemaURI)
  }

  def getDataFrameDocument(dataFrameName: String): DataFrameDocument = {
    val dataFrameDocument = flightClient.doAction(new Action(s"getDataFrameDocument.$dataFrameName")).asScala
    SimpleSerializer.deserialize(getArrayBytesResult(dataFrameDocument)).asInstanceOf[DataFrameDocument]
  }

  def close(): Unit = {
    flightClient.close()
  }

  def getRows(dataFrameName: String, operationNode: String): Iterator[Row] = {
    //上传参数
//    val paramFields: Seq[Field] = List(
//      new Field("dfName", FieldType.nullable(new ArrowType.Utf8()), null),
//      new Field("userToken", FieldType.nullable(new ArrowType.Utf8()), null),
//      new Field("DFOperation", FieldType.nullable(new ArrowType.Utf8()), null)
//    )
//    val schema = new Schema(paramFields.asJava)
//    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
//    val dfNameCharVector = vectorSchemaRoot.getVector("dfName").asInstanceOf[VarCharVector]
//    val tokenCharVector = vectorSchemaRoot.getVector("userToken").asInstanceOf[VarCharVector]
//    val dfOperationVector = vectorSchemaRoot.getVector("DFOperation").asInstanceOf[VarCharVector]
//    dfNameCharVector.allocateNew(1)
//    dfNameCharVector.set(0, dataFrameName.getBytes("UTF-8"))
//    tokenCharVector.allocateNew(1)
//    tokenCharVector.set(0, userToken.orNull.getBytes("UTF-8"))
//    dfOperationVector.allocateNew(1)
//    dfOperationVector.set(0, operationNode.getBytes("UTF-8"))
//    vectorSchemaRoot.setRowCount(1)
//
//    val requestSchemaId = UUID.randomUUID().toString
//    val listener = flightClient.startPut(FlightDescriptor.path(requestSchemaId), vectorSchemaRoot, new AsyncPutListener())
//    listener.putNext()
//    listener.completed()
//    listener.getResult()

    val result = flightClient.doAction(new Action(s"putRequest:$dataFrameName:${userToken.orNull}", operationNode.getBytes("UTF-8"))).asScala
    result.hasNext
    val flightInfo = flightClient.getInfo(FlightDescriptor.path(userToken.orNull))
    val flightInfoSchema = flightInfo.getSchema
    val isBinaryColumn = flightInfoSchema.getFields.last.getType match {
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
        Seq.range(0, rowCount).map(index => {
          val rowMap = mutable.LinkedHashMap(fieldVectors.map(vec => {
            if (vec.isNull(index)) (vec.getName, null)
            else vec match {
              case v: org.apache.arrow.vector.IntVector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.BigIntVector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.VarCharVector => (vec.getName, new String(v.get(index)))
              case v: org.apache.arrow.vector.Float8Vector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.BitVector => (vec.getName, v.get(index) == 1)
              case v: org.apache.arrow.vector.VarBinaryVector => (vec.getName, v.get(index))
              case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getClass}")
            }
          }): _*)
          val r: Seq[Any] = rowMap.values.toList
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
      var currentSeq: Seq[Any] = if (flatIter.hasNext) flatIter.next() else Seq.empty[Any]
      var cachedSeq: Seq[Any] = currentSeq
      var currentChunk: Array[Byte] = Array[Byte]()
      var cachedChunk: Array[Byte] = currentSeq.last.asInstanceOf[Array[Byte]]
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
                  val nextSeq: Seq[Any] = if (flatIter.hasNext) flatIter.next() else Seq.empty[Any]
                  val nextName: String = nextSeq(0).asInstanceOf[String]
                  val nextChunk: Array[Byte] = nextSeq.last.asInstanceOf[Array[Byte]]
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
          Row(currentSeq.init :+ new Blob(blobIter, currentSeq(0).asInstanceOf[String]): _*)
        }
      }
    }
  }

  private def getListStringByResult(resultIterator: Iterator[Result]): Seq[String] = {
    if (resultIterator.hasNext) {
      val result = resultIterator.next
      val vectorSchemaRootReceived = DataUtils.getVectorSchemaRootFromBytes(result.getBody, allocator)
      val rowCount = vectorSchemaRootReceived.getRowCount
      val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
      Seq.range(0, rowCount).map(index => {
        val rowMap = fieldVectors.map(vec => {
          vec.asInstanceOf[VarCharVector].getObject(index).toString
        }).head
        rowMap
      })
    } else null
  }

  private def getSingleStringByResult(resultIterator: Iterator[Result]): String = {
    if (resultIterator.hasNext) {
      val result = resultIterator.next
      val vectorSchemaRootReceived = DataUtils.getVectorSchemaRootFromBytes(result.getBody, allocator)
      val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
      fieldVectors.head.asInstanceOf[VarCharVector].getObject(0).toString
    } else null
  }

  private def getSingleLongByResult(resultIterator: Iterator[Result]): Long = {
    if (resultIterator.hasNext) {
      val result = resultIterator.next
      val vectorSchemaRootReceived = DataUtils.getVectorSchemaRootFromBytes(result.getBody, allocator)
      val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
      fieldVectors.head.asInstanceOf[BigIntVector].getObject(0)
    } else 0L
  }

  private def getMapByJsonString(hostInfoString: String): Map[String, String] = {
    val parseResult: Either[io.circe.Error, Map[String, String]] = parser.parse(hostInfoString).flatMap { json =>
      json.asObject.toRight(
        // 如果不是对象，则返回一个 DecodingFailure
        DecodingFailure("JSON is not an object", List.empty)
      ).map { jsonObject =>
        // 将 JsonObject 转换为 Map[String, Json]
        jsonObject.toMap.mapValues {
          case jsonValue if jsonValue.isString => jsonValue.asString.get // 如果是 JSON 字符串，直接取其值
          case other => other.toString() // 修改为 toString() 以兼容其他 Json 类型
        }
      }
    }
    parseResult.fold(
      error => throw new RuntimeException(s"Error parsing JSON: $error"),
      identity // 如果解析成功，直接返回结果
    )
  }

  private def getArrayBytesResult(resultIterator: Iterator[Result]): Array[Byte] = {
    if (resultIterator.hasNext) {
      val result = resultIterator.next
      val vectorSchemaRootReceived = DataUtils.getVectorSchemaRootFromBytes(result.getBody, allocator)
      val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
      fieldVectors.head.asInstanceOf[VarBinaryVector].getObject(0)
    } else null
  }

}

// 表示完整的二进制文件
class Blob(val chunkIterator: Iterator[Array[Byte]], val name: String) extends Serializable {
  // 缓存加载后的完整数据
  private var _content: Option[Array[Byte]] = None
  // 缓存文件大小（独立于_content，避免获取大数组长度）
  private var _size: Option[Long] = None

  private var _memoryReleased: Boolean = false

  private def loadLazily(): Unit = {
    val byteStream = new ByteArrayOutputStream()
    var totalSize: Long = 0L
    var chunkCount = 0
    try {
      if (_content.isEmpty && _size.isEmpty) {
        while (chunkIterator.hasNext) {
          val chunk = chunkIterator.next()
          totalSize += chunk.length
          chunkCount += 1
          byteStream.write(chunk)

        }
        _content = Some(byteStream.toByteArray)
        _size = Some(totalSize)
      }
    }
    catch{
        case e: OutOfMemoryError => {
          _size = Some(writeToFile(name))
          _content = None
        }
    } finally {
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

  /** 释放content占用的内存 */
  def releaseContentMemory(): Unit = {
    _content = None
    _memoryReleased = true
    System.gc()
  }

  // 获得 `InputStream`（适合流式读取 `content`）
  def getInputStream: InputStream = {
    if (_memoryReleased) throw new IllegalStateException("Blob content memory has been released")
    if (_content.isEmpty) loadLazily()
    new ByteArrayInputStream(_content.get)
  }

  // 将 Blob 内容写入指定文件（返回写入的字节数）
  def writeToFile(pathString: String = Paths.get("src","test","demo","data","output").toString, nameString: String = name): Long = {
    val path = Paths.get(pathString,nameString)
    if (_memoryReleased) throw new IllegalStateException("Blob content memory has been released")
    if (_size.isEmpty) loadLazily()
    val inputStream = getInputStream
    val outputStream = new FileOutputStream(path.toFile)
    try {
      var bytesWritten: Long = 0L
      val buffer = new Array[Byte](4096) // 4KB 缓冲区
      var bytesRead: Int = 0
      // **流式写入，避免全部加载到内存**
      while ( {
        bytesRead = inputStream.read(buffer)
        bytesRead != -1
      }) {
        outputStream.write(buffer, 0, bytesRead)
        bytesWritten += bytesRead
      }
      bytesWritten // 返回实际写入的字节数
    } finally {
      inputStream.close()
      outputStream.close()
    }
  }

  /** 获取分块迭代器 */
  //  def chunkIterator: Iterator[Array[Byte]] = chunkIterator

  override def toString: String = {
    loadLazily()
    s"Blob[$name]"
  }
}