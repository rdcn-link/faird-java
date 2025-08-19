package link.rdcn.client

import link.rdcn.struct.{Blob, DataFrame, Row, StructType}
import link.rdcn.user.Credentials
import link.rdcn.util.ClientUtils.getSingleStringByResult
import link.rdcn.util.{ClientUtils, ClosableIterator}
import org.apache.arrow.flight.{Action, FlightClient, Location, Ticket}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import java.io.{FileInputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.{Properties, UUID}
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter, seqAsJavaListConverter}
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 19:47
 * @Modified By:
 */
abstract class DftpClient(url: String, port: Int, useTLS: Boolean = false) {

  val location = {
    if (useTLS) {
      val props = new Properties()
      val confPathURI = this.getClass.getProtectionDomain().getCodeSource().getLocation().toURI
      val fis = new InputStreamReader(new FileInputStream(Paths.get(confPathURI).resolve("user.conf").toString), "UTF-8")
      try props.load(fis) finally fis.close()
      System.setProperty("javax.net.ssl.trustStore", Paths.get(props.getProperty("tls.path")).toString())
      Location.forGrpcTls(url, port)
    } else
      Location.forGrpcInsecure(url, port)
  }
  val allocator: BufferAllocator = new RootAllocator()
  ClientUtils.init(allocator)
  val flightClient = FlightClient.builder(allocator, location).build()

  def login(credentials: Credentials): Unit = {
    flightClient.authenticate(new FlightClientAuthHandler(credentials))
  }

  def get(url: String, body: Array[Byte] = Array.emptyByteArray): DataFrame

  def put(dataFrame: DataFrame): Unit

  def getRows(dataFrameName: String, operationNode: String): (StructType,ClosableIterator[Row]) = {
    val dataFrameRequestKey = UUID.randomUUID().toString
    //上传参数
    val result = flightClient.doAction(new Action(s"/putRequest:$dataFrameName:${dataFrameRequestKey}", operationNode.getBytes("UTF-8"))).asScala
    result.hasNext

    val isBinaryColumn = false
    val flightStream = flightClient.getStream(new Ticket(dataFrameRequestKey.getBytes(StandardCharsets.UTF_8)))
    val vectorSchemaRootReceived = flightStream.getRoot
    val schema = ClientUtils.arrowSchemaToStructType(vectorSchemaRootReceived.getSchema)
    val iter: Iterator[Seq[Seq[Any]]] = new Iterator[Seq[Seq[Any]]] {
      override def hasNext: Boolean = flightStream.next()

      override def next(): Seq[Seq[Any]] = {
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

    val stream = if (!isBinaryColumn) {
      // 最后一列不是binary类型，直接返回Row(Seq[Any])
      flatIter.map(seq => Row.fromSeq(seq))
    } else {

      var isFirstChunk: Boolean = true
      var isCalled: Boolean = false
      var currentSeq: Seq[Any] = if (flatIter.hasNext) flatIter.next() else Seq.empty[Any]
      var cachedSeq: Seq[Any] = currentSeq
      var currentChunk: Array[Byte] = Array[Byte]()
      var cachedChunk: Array[Byte] = currentSeq.last.asInstanceOf[Array[Byte]]
      val verifyChunk: Array[Byte] = currentSeq.last.asInstanceOf[Array[Byte]]
      var cachedName: String = currentSeq(0).asInstanceOf[String]
      var currentName: String = currentSeq(0).asInstanceOf[String]
      new Iterator[Row] {
        override def hasNext: Boolean = {
          if (isCalled) {
            !(verifyChunk eq cachedChunk) && (flatIter.hasNext || cachedChunk.nonEmpty)
          } else {
            isCalled = true
            (flatIter.hasNext || cachedChunk.nonEmpty)
          }
        }

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
    if (!isBinaryColumn)
      (schema, ClosableIterator(stream)())
    else
      (schema, new ClosableIterator(stream, ()=>{},true))
  }

  def close(): Unit = flightClient.close()

}
