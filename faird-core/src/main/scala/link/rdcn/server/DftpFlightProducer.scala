package link.rdcn.server

import link.rdcn.ErrorCode.USER_NOT_LOGGED_IN
import link.rdcn.dftree.Operation
import link.rdcn.server.exception.{AuthorizationException, DataFrameAccessDeniedException}
import link.rdcn.Logging
import link.rdcn.user.DataOperationType
import link.rdcn.util.ServerUtils
import link.rdcn.util.ServerUtils.convertStructTypeToArrowSchema
import org.apache.arrow.flight.{Action, Criteria, FlightDescriptor, FlightEndpoint, FlightInfo, FlightProducer, Location, NoOpFlightProducer, Result, Ticket}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.Schema

import java.nio.charset.StandardCharsets
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 14:36
 * @Modified By:
 */
class DftpFlightProducer(allocator: BufferAllocator, location: Location, dftpServer: DftpServer) extends NoOpFlightProducer with Logging {

  private val batchLen = 100

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
    //dftp 处理 df.map.filter ……
    if(action.getType.startsWith("/putRequest")){
      val userToken: String = context.peerIdentity()

      val dfName =  action.getType.split(":")(1)
      val requestDataFrameKey = action.getType.split(":")(2)
      val authenticatedUser = Option(dftpServer.authenticatedUserMap.get(userToken))
      if(authenticatedUser.isEmpty){
        throw new AuthorizationException(USER_NOT_LOGGED_IN)
      }
      if(! dftpServer.authProvider.checkPermission(authenticatedUser.get, dfName, List.empty[DataOperationType].asJava.asInstanceOf[java.util.List[DataOperationType]] ))
        throw new DataFrameAccessDeniedException(dfName)
      val operationNode: Operation = Operation.fromJsonString(new String(action.getBody, StandardCharsets.UTF_8))
      dftpServer.requestMap.put(requestDataFrameKey, (dfName, operationNode))
      listener.onCompleted()
    }else{
      val request = DftpRequest(action.getType, ActionType.GET, action.getBody)
      val response = DftpResponse(200)
      dftpServer.doGet(request,response)
      val dataFrame = response.dataFrame
      if(dataFrame.nonEmpty) ServerUtils.sendDataFrame(dataFrame.get, listener) else listener.onCompleted()
    }
  }

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
    val dataFrameRequestKey = new String(ticket.getBytes, StandardCharsets.UTF_8)

    val request = DftpRequest("/get/"+dataFrameRequestKey, ActionType.GET)
    val response = DftpResponse(200)
    dftpServer.doGet(request, response)
    val outDataFrame = response.dataFrame
    outDataFrame match {
      case Some(dataFrame) =>
        val schema = convertStructTypeToArrowSchema(dataFrame.schema)
        val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(schema, childAllocator)
        val loader = new VectorLoader(root)
        listener.start(root)

        val arrowFlightStreamWriter = ArrowFlightStreamWriter(dataFrame)
        try {
          arrowFlightStreamWriter.process(root, batchLen).foreach(batch => {
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
      case None =>
        response.send(500)
        listener.error(new IllegalStateException(s"Request ${request.path} returned ${response.code}, message=${response.message.getOrElse("")}"
      ))
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
}
