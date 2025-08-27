package link.rdcn.client.dftp

import link.rdcn.client.dag._
import link.rdcn.client._
import link.rdcn.dftree.FunctionWrapper.RepositoryOperator
import link.rdcn.dftree._
import link.rdcn.server.dftp.ArrowFlightStreamWriter
import link.rdcn.struct._
import link.rdcn.user.Credentials
import link.rdcn.util.{ClientUtils, ClosableIterator, CodecUtils, IteratorInputStream, ServerUtils}
import org.apache.arrow.flight.{Action, PutResult, FlightClient, FlightDescriptor, Location, SyncPutListener, Ticket}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.json.JSONObject

import java.io.{FileInputStream, InputStream, InputStreamReader}
import java.nio.file.Paths
import java.util.Properties
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 19:47
 * @Modified By:
 */
class DftpClient (url: String, port: Int, useTLS: Boolean = false) {
  val prefixSchema: String = "dftp"
  def login(credentials: Credentials): Unit = {
    flightClient.authenticate(new FlightClientAuthHandler(credentials))
  }

  def doAction(actionName: String, params: Array[Byte] = Array.emptyByteArray, paramMap: Map[String, Any] = Map.empty): DataFrame = {
    val body = CodecUtils.encodeWithMap(params, paramMap)
    val actionResult =  flightClient.doAction(new Action(actionName, body))
    ClientUtils.parseFlightActionResults(actionResult, allocator)
  }

  def get(url: String): DataFrame = {
    val urlValidator = new UrlValidator(prefixSchema)
    urlValidator.extractPath(url) match {
      case Right(path) => RemoteDataFrameProxy(path, getRows)
      case Left(message) => throw new IllegalArgumentException(message)
    }
  }

  def put(dataFrame: DataFrame, setDataBatchLen: Int = 100): String = {
    val arrowSchema = ServerUtils.convertStructTypeToArrowSchema(dataFrame.schema)
    val childAllocator = allocator.newChildAllocator("put-data-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, childAllocator)
    val putListener = new SyncPutListener
    val writer = flightClient.startPut(FlightDescriptor.path(""), root, putListener)
    val loader = new VectorLoader(root)
    dataFrame.mapIterator(iter => {
      val arrowFlightStreamWriter = ArrowFlightStreamWriter(iter)
      try {
        arrowFlightStreamWriter.process(root, setDataBatchLen).foreach(batch => {
          try {
            loader.load(batch)
            while (!writer.isReady()) {
              LockSupport.parkNanos(1)
            }
            writer.putNext()
          } finally {
            batch.close()
          }
        })
        writer.completed()
        val ack: PutResult = putListener.read()
        var result = ""
        if (ack != null) {
          val metadataBuf = ack.getApplicationMetadata
          val bytes = new Array[Byte](metadataBuf.readableBytes().toInt)
          metadataBuf.readBytes(bytes)
          result = CodecUtils.decodeString(bytes)
          ack.close()
        }
        // 等待服务端最终完成信号
        putListener.getResult()
        result
      } catch {
        case e: Throwable => writer.error(e)
          throw e
      } finally {
        if (root != null) root.close()
        if (childAllocator != null) childAllocator.close()
      }
    })
  }

  def close(): Unit = flightClient.close()

  //执行 DAG
  def execute(transformerDAG: Flow): ExecutionResult = {
    val executePaths = transformerDAG.getExecutionPaths()
    val dfs: Seq[DataFrame] = executePaths.map(path => getRemoteDataFrameByDAGPath(path))
    new ExecutionResult() {
      override def single(): DataFrame = dfs.head

      override def get(name: String): DataFrame = dfs(name.toInt-1)

      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
        case (dataFrame, id) => (id.toString, dataFrame)
      }.toMap
    }
  }

  private val location = {
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
  private val allocator: BufferAllocator = new RootAllocator()
  private val flightClient: FlightClient = FlightClient.builder(allocator, location).build()
  private def getRows(url: String, operationNode: String): (StructType, ClosableIterator[Row]) = {
    val schemaAndIter = getStream(flightClient, new Ticket(CodecUtils.encodeTicket(CodecUtils.URL_STREAM ,url, operationNode)))
    val stream = schemaAndIter._2.map(seq => Row.fromSeq(seq))
    (schemaAndIter._1, ClosableIterator(stream)())
  }

  private def getStream(flightClient: FlightClient, ticket: Ticket): (StructType, Iterator[Seq[Any]]) = {
    val flightStream = flightClient.getStream(ticket)
    val vectorSchemaRootReceived = flightStream.getRoot
    val schema = ClientUtils.arrowSchemaToStructType(vectorSchemaRootReceived.getSchema)
    val iter = new Iterator[Seq[Seq[Any]]] {
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
                case v: org.apache.arrow.vector.VarCharVector =>
                  if(v.getField.getMetadata.isEmpty)
                    (vec.getName, new String(v.get(index)))
                  else (vec.getName, DFRef(new String(v.get(index))))
                case v: org.apache.arrow.vector.Float8Vector => (vec.getName, v.get(index))
                case v: org.apache.arrow.vector.BitVector => (vec.getName, v.get(index) == 1)
                case v: org.apache.arrow.vector.VarBinaryVector =>
                  if(v.getField.getMetadata.isEmpty) (vec.getName, v.get(index))
                  else {
                    val blobId = CodecUtils.decodeString(v.get(index))
                    val blobTicket =  new Ticket(CodecUtils.encodeTicket(CodecUtils.BLOB_STREAM , blobId, ""))
                    val blob = new Blob {
                      val iter = getStream(flightClient, blobTicket)._2
                      val chunkIterator = iter.map(value => {
                        value.head match{
                          case v: Array[Byte] => v
                          case other => throw new Exception(s"Blob parsing failed: expected Array[Byte], but got ${other}")
                        }
                      })

                      override def offerStream[T](consume: InputStream => T): T = {
                        val stream = new IteratorInputStream(chunkIterator)
                        try consume(stream)
                        finally stream.close()
                      }
                    }
                    (vec.getName, blob)
                  }
                case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getClass}")
              }
            }): _*)
            rowMap.values.toList
          })
        }
      }.flatMap(batchRows => batchRows)
      (schema, iter)
    }

  private def getRemoteDataFrameByDAGPath(path: Seq[FlowNode]): DataFrame = {
    val dataFrameName = path.head.asInstanceOf[SourceNode].dataFrameName
    var operation: Operation = SourceOp()
    path.foreach(node => node match {
      case f: Transformer11 =>
        val genericFunctionCall = DataFrameCall(new SerializableFunction[DataFrame, DataFrame] {
          override def apply(v1: DataFrame): DataFrame = f.transform(v1)
        })
        val transformerNode: TransformerNode = TransformerNode(FunctionWrapper.getJavaSerialized(genericFunctionCall), operation)
        operation = transformerNode
      case node: RepositoryNode =>
        val jo = new JSONObject()
        jo.put("type", LangType.REPOSITORY_OPERATOR.name)
        jo.put("functionID", node.functionId)
        val transformerNode: TransformerNode = TransformerNode(FunctionWrapper(jo).asInstanceOf[RepositoryOperator], operation)
        operation = transformerNode
      case s: SourceNode => // 不做处理
      case _ => throw new IllegalArgumentException(s"This FlowNode ${node} is not supported please extend Transformer11 trait")
    })
    RemoteDataFrameProxy(s"dacp://$url:$port"+dataFrameName, getRows, operation)
  }
}


object DftpClient {

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): DftpClient = {
    UrlValidator.extractBase(url) match {
      case Some(parsed) =>
        val client = new DftpClient(parsed._2, parsed._3)
        client.login(credentials)
        client
      case None =>
        throw new IllegalArgumentException(s"Invalid DFTP URL: $url")
    }
  }
  def connectTLS(url: String, credentials: Credentials = Credentials.ANONYMOUS): DftpClient = {
    UrlValidator.extractBase(url) match {
      case Some(parsed) =>
        val client = new DftpClient(parsed._2, parsed._3, true)
        client.login(credentials)
        client
      case None =>
        throw new IllegalArgumentException(s"Invalid DFTP URL: $url")
    }
  }
}
