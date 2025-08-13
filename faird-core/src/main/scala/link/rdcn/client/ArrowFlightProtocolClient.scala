package link.rdcn.client

import link.rdcn.SimpleSerializer
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics}
import link.rdcn.struct.{Blob, DataFrame, Row}
import link.rdcn.user.{AnonymousCredentials, Credentials, UsernamePassword}
import link.rdcn.util.{ClientUtils, ClosableIterator}
import link.rdcn.util.ClientUtils._
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.Lang

import java.io.{FileInputStream, InputStreamReader, StringReader}
import java.nio.file.Paths
import java.util.Properties
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

  def getDataSetMetaData(dsName: String): Model

  def close(): Unit

  def getRows(dataFrameName: String, ops: String): Iterator[Row]

  def getDataFrameSize(dataFrameName: String): Long

  def getHostInfo: Map[String, String]

  def getServerResourceInfo: Map[String, String]

  def put(dataFrame: DataFrame): Unit
}

class ArrowFlightProtocolClient(url: String, port: Int, useTLS: Boolean = false) extends ProtocolClient {

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
  private val flightClient = FlightClient.builder(allocator, location).build()
  private var userToken: Option[String] = None

  def login(credentials: Credentials): Unit = {
    val clientAuthHandler = new FlightClientAuthHandler(credentials)
    flightClient.authenticate(clientAuthHandler)
    userToken = Some(clientAuthHandler.getSessionToken)
    val paramFields: Seq[Field] = List(
      new Field("credentials", FieldType.nullable(new ArrowType.Utf8), null),
    )
    val schema = new Schema(paramFields.asJava)
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val credentialsVector = vectorSchemaRoot.getVector("credentials").asInstanceOf[VarCharVector]
    credentialsVector.allocateNew(1)
    val userPassword = credentials match {
      case _: UsernamePassword => credentials.asInstanceOf[UsernamePassword]
      case AnonymousCredentials => UsernamePassword("anonymous", "anonymous")
      case _ => throw new IllegalArgumentException("Unsupported credential type")
    }
    val credentialsJsonString =
      s"""
       |{
       |    "username" : "${userPassword.username}",
       |    "password" : "${userPassword.password}"
       |}
       |""".stripMargin.stripMargin.replaceAll("\n", "").replaceAll("\\s+", " ")
    credentialsVector.set(0, credentialsJsonString.getBytes("UTF-8"))
    vectorSchemaRoot.setRowCount(1)
    val body = getBytesFromVectorSchemaRoot(vectorSchemaRoot)
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

  override def getDataSetMetaData(dataSetName: String): Model = {
    val dataSetMetaData = flightClient.doAction(new Action(s"getDataSetMetaData.$dataSetName")).asScala
    val modelString = getSingleStringByResult(dataSetMetaData)
    val model = ModelFactory.createDefaultModel
    val reader = new StringReader(modelString)
    model.read(reader, null, Lang.TTL.getName)
    model
  }

  override def getDataFrameSize(dataFrameName: String): Long = {
    val dataFrameSize = flightClient.doAction(new Action(s"getDataFrameSize.$dataFrameName")).asScala
    getSingleLongByResult(dataFrameSize)
  }

  override def getHostInfo: Map[String, String] = {
    val hostInfo = flightClient.doAction(new Action(s"getHostInfo")).asScala
    getMapByJsonString(getSingleStringByResult(hostInfo))
  }

  override def getServerResourceInfo: Map[String, String] = {
    val serverResourceInfo = flightClient.doAction(new Action(s"getServerResourceInfo")).asScala
    getMapByJsonString(getSingleStringByResult(serverResourceInfo))
  }

  def getSchemaURI(dataFrameName: String): String = {
    val schemaURI = flightClient.doAction(new Action(s"getSchemaURI.$dataFrameName")).asScala
    getSingleStringByResult(schemaURI)
  }

  def getDocument(dataFrameName: String): DataFrameDocument = {
    val dataFrameDocument = flightClient.doAction(new Action(s"getDocument.$dataFrameName")).asScala
    SimpleSerializer.deserialize(getArrayBytesResult(dataFrameDocument)).asInstanceOf[DataFrameDocument]
  }

  def getStatistics(dataFrameName: String): DataFrameStatistics = {
        val dataFrameStatistics = flightClient.doAction(new Action(s"getStatistics.$dataFrameName")).asScala
        SimpleSerializer.deserialize(getArrayBytesResult(dataFrameStatistics)).asInstanceOf[DataFrameStatistics]
  }

  def close(): Unit = {
    flightClient.close()
  }

  def getRows(dataFrameName: String, operationNode: String): ClosableIterator[Row] = {
    //上传参数
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
      ClosableIterator(stream)()
    else
      new ClosableIterator(stream, ()=>{},true)
  }

  override def put(dataFrame: DataFrame): Unit = ???
}