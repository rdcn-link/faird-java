package link.rdcn.server.dftp

import link.rdcn.Logging
import link.rdcn.dftree.Operation
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrame, DefaultDataFrame, Row, StructType}
import link.rdcn.util.ServerUtils.convertStructTypeToArrowSchema
import link.rdcn.util.{ClientUtils, ClosableIterator, CodecUtils, DataUtils, ServerUtils}
import org.apache.arrow.flight._
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}

import java.nio.charset.StandardCharsets
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 14:36
 * @Modified By:
 */
trait GetRequest {
  def getRequestedUrl(): String
  def getTransformer(): Operation
}

trait GetResponse{
  def sendDataFrame(dataFrame: DataFrame): Unit
  def sendError(code: Int, message: String): Unit
}

trait ActionRequest{
  def getActionName(): String
  def getActionParameters(): Array[Byte]
  def getActionParameterMap(): Map[String, Any]
}

trait ActionResponse{
  def sendDataFrame(dataFrame: DataFrame): Unit
  def sendError(code: Int, message: String): Unit
}

trait PutRequest{
  def getDataFrame(): DataFrame
}

trait PutResponse{
  def sendBinary(bytes: Array[Byte]): Unit
  def sendMap(data: Map[String, Any]): Unit
  def sendJsonString(jsonStr: String): Unit
  def sendError(code: Int, message: String): Unit
}

class DftpFlightProducer(allocator: BufferAllocator, location: Location, dftpServer: DftpServer) extends NoOpFlightProducer with Logging {

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
    val actionResponse = new ActionResponse {
      override def sendDataFrame(dataFrame: DataFrame): Unit =  ServerUtils.sendDataFrame(dataFrame, listener)

      override def sendError(code: Int, message: String): Unit = listener.onError(new Exception(message))
    }
    val body = CodecUtils.decodeWithMap(action.getBody)
    val actionRequest = new ActionRequest {
      override def getActionName(): String = action.getType

      override def getActionParameters(): Array[Byte] = body._1

      override def getActionParameterMap(): Map[String, Any] = body._2
    }
    dftpServer.getDftpServiceHandler().doAction(actionRequest,actionResponse)
  }

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    val setDataBatchLen = 100
    val ticketStr = CodecUtils.decodePair(ticket.getBytes)
    val request = new GetRequest {
      override def getRequestedUrl(): String = ticketStr._1
      override def getTransformer(): Operation = Operation.fromJsonString(ticketStr._2)
    }
    val response = new GetResponse {
      override def sendDataFrame(dataFrame: DataFrame): Unit = {
        val schema = convertStructTypeToArrowSchema(dataFrame.schema)
        val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(schema, childAllocator)
        val loader = new VectorLoader(root)
        listener.start(root)
        dataFrame.mapIterator(iter => {
          val arrowFlightStreamWriter = ArrowFlightStreamWriter(iter)
          try {
            arrowFlightStreamWriter.process(root, setDataBatchLen).foreach(batch => {
              try {
                loader.load(batch)
                while (!listener.isReady()) {
                  LockSupport.parkNanos(1)
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
          }
        })
      }

      override def sendError(code: Int, message: String): Unit = listener.error(new Exception(message))
    }
    if(ticketStr._1 == "getBlob"){
      val blobId = ticketStr._2
      val blob = BlobRegistry.getBlob(blobId)
      if(blob.isEmpty){response.sendError(404, s"blob ${blobId} resource closed")}
      else {
        blob.get.offerStream(inputStream => {
          val stream: Iterator[Row] = DataUtils.chunkedIterator(inputStream).map(bytes => Row.fromSeq(Seq(bytes)))
          val schema = StructType.empty.add("content", StringType)
          response.sendDataFrame(DefaultDataFrame(schema, stream))
        })
      }
    }
    dftpServer.get(request, response)
  }

  override def acceptPut(
                          context: FlightProducer.CallContext,
                          flightStream: FlightStream,
                          ackStream: FlightProducer.StreamListener[PutResult]
                        ): Runnable = {
    new Runnable {
      override def run(): Unit = {
        val request = new PutRequest {
          override def getDataFrame(): DataFrame = {
            var schema = StructType.empty
            if(flightStream.next()){
              val root = flightStream.getRoot
              schema = ClientUtils.arrowSchemaToStructType(root.getSchema)
              val stream = ServerUtils.flightStreamToRowIterator(flightStream)
              new DataFrameWithArrowRoot(root, schema, stream)
            }else{DefaultDataFrame(schema, Iterator.empty) }
          }
        }
        val response = new PutResponse {
          //TODO 统一编码和解码格式
          override def sendBinary(bytes: Array[Byte]): Unit = sendBytes(bytes, ackStream)

          override def sendMap(data: Map[String, Any]): Unit = sendBytes(CodecUtils.encodeMap(data), ackStream)

          override def sendJsonString(jsonStr: String): Unit = sendBytes(CodecUtils.encodeString(jsonStr), ackStream)

          override def sendError(code: Int, message: String): Unit = ackStream.onError(new Exception(message))
        }
        dftpServer.getDftpServiceHandler().doPut(request, response)
      }
    }
  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
    val schema = new Schema(List.empty.asJava)
    new FlightInfo(schema, descriptor, List(flightEndpoint).asJava, -1L, 0L)
  }

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
    listener.onCompleted()
  }

  private def sendBytes(bytes: Array[Byte], ackStream: FlightProducer.StreamListener[PutResult]): Unit = {
    val buf: ArrowBuf = allocator.buffer(bytes.length)
    try {
      buf.writeBytes(bytes)
      ackStream.onNext(PutResult.metadata(buf))
      ackStream.onCompleted()
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        ackStream.onError(e)
    } finally {
      buf.close()
    }
  }
}
