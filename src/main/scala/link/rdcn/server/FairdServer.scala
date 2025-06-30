package link.rdcn.server

import io.grpc.StatusRuntimeException
import link.rdcn.client.{DFOperation, RemoteDataFrameImpl}
import link.rdcn.util.DataUtils
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import link.rdcn.{ConfigLoader, Logging, SimpleSerializer}
import link.rdcn.provider.{DataProvider, DataProviderImplByDataSetList, DataStreamSource}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials}
import link.rdcn.util.DataUtils.convertStructTypeToArrowSchema
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 18:36
 * @Modified By:
 */

//object FairdServer extends App with Logging {
//  val location = Location.forGrpcInsecure(ConfigLoader.fairdConfig.getHostPosition, ConfigLoader.fairdConfig.getHostPort)
//  val allocator: BufferAllocator = new RootAllocator()
//
//  try {
//    val producer = new FlightProducerImpl(allocator, location, )
//    val flightServer = FlightServer.builder(allocator, location, producer).build()
//
//    flightServer.start()
//    Runtime.getRuntime.addShutdownHook(new Thread() {
//      override def run(): Unit = {
//        flightServer.close()
//        producer.close()
//      }
//    })
//    flightServer.awaitTermination()
//  } catch {
//    case e: Exception => e.printStackTrace()
//  }
//}

class FlightProducerImpl(allocator: BufferAllocator, location: Location, dataProvider: DataProvider, authProvider: AuthProvider) extends NoOpFlightProducer with Logging {

  private val requestMap = new ConcurrentHashMap[FlightDescriptor, RemoteDataFrameImpl]()
  private val authenticatedUserMap = new ConcurrentHashMap[String, AuthenticatedUser]()
  private val batchLen = 100

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {

    new Runnable {
      override def run(): Unit = {
        val ticketKey: String = flightStream.getDescriptor.getPath.get(0)
        ticketKey match {
          case ticketKey if ticketKey.startsWith("login") =>
            if(flightStream.next()){
              val root = flightStream.getRoot
              val credentialsBytes = root.getFieldVectors.get(0).asInstanceOf[VarBinaryVector].getObject(0)
              val credentials = SimpleSerializer.deserialize(credentialsBytes).asInstanceOf[Credentials]
              val authenticatedUser: AuthenticatedUser = authProvider.authenticate(credentials)
              val loginToken: String = ticketKey.split("\\.")(1)
              authenticatedUserMap.put(loginToken, authenticatedUser)
              flightStream.getRoot.clear()
            }
          case _ => {
            while (flightStream.next()) {
              val root = flightStream.getRoot
              val rowCount = root.getRowCount
              val dfName = root.getFieldVectors.get(0).asInstanceOf[VarCharVector].getObject(0).toString
              val userToken = root.getFieldVectors.get(1).asInstanceOf[VarCharVector].getObject(0).toString
              val authenticatedUser = Option(authenticatedUserMap.get(userToken))
              if(authenticatedUser.isEmpty){
               throw new Exception(s"The user $userToken is not logged in")
              }
              if(! authProvider.authorize(authenticatedUser.get, dfName))
                throw new StatusRuntimeException(io.grpc.Status.NOT_FOUND.withDescription(s"不允许访问$dfName"))
              val dfOperations: List[DFOperation] = List.range(0, rowCount).map(index => {
                val bytes = root.getFieldVectors.get(2).asInstanceOf[VarBinaryVector].get(index)
                if (bytes == null) null else
                  SimpleSerializer.deserialize(bytes).asInstanceOf[DFOperation]
              })
              val remoteDataFrameImpl = if (dfOperations.contains(null)) RemoteDataFrameImpl(dfName, List.empty)
              else RemoteDataFrameImpl(dfName, ops = dfOperations)
              requestMap.put(flightStream.getDescriptor, remoteDataFrameImpl)
              flightStream.getRoot.clear()
            }
          }
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


  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
        new String(ticket.getBytes, StandardCharsets.UTF_8) match {
          case "listDataSetNames" => getListStrStream(dataProvider.listDataSetNames().asScala, listener)
          case ticketKey if ticketKey.startsWith("listDataFrameNames") => {
            val dataSet = ticketKey.replace("listDataFrameNames.","")
            getListStrStream(dataProvider.listDataFrameNames(dataSet).asScala, listener)
          }
          case ticketKey if ticketKey.startsWith("getSchemaURI") => {
            val dfName = ticketKey.replace("getSchemaURI.","")
            getStrStream(dataProvider.getDataFrameSchemaURL(dfName),listener)

          }
          case ticketKey if ticketKey.startsWith("getSchema") => {
            val dfName =  ticketKey.replace("getSchema.","")
            getStrStream(dataProvider.getDataFrameSchema(dfName).toString,listener)
          }
          case ticketKey if ticketKey.startsWith("getDataSetMetaData") => {
            val dsName = ticketKey.replace("getDataSetMetaData.","")
            val model: Model = ModelFactory.createDefaultModel()
            dataProvider.getDataSetMetaData(dsName, model)
            getStrStream(model.toString,listener)
          }
          case _ => {
            val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))
            val request: RemoteDataFrameImpl = requestMap.get(flightDescriptor)

            val dataStreamSource: DataStreamSource = dataProvider.getDataFrameSource(request.dataFrameName)
            val structType = dataProvider.getDataFrameSchema(request.dataFrameName)
            val schema = convertStructTypeToArrowSchema(structType)

            //能否支持并发
            val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
            val root = VectorSchemaRoot.create(schema, childAllocator)
            val loader = new VectorLoader(root)
            listener.start(root)

            val stream = dataStreamSource.createDataFrame().execute(request.ops)
            try {
              dataStreamSource.process(stream, root, batchLen).foreach(batch => {
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
              requestMap.remove(flightDescriptor)
            }
          }
    }

  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {

      val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
      val request = requestMap.getOrDefault(descriptor, null)
      val schema =  if (request != null)
        convertStructTypeToArrowSchema(dataProvider.getDataFrameSchema(request.dataFrameName))
      else new Schema(List.empty.asJava)
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

}
