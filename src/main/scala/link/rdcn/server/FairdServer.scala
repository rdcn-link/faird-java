package link.rdcn.server

import com.sun.management.OperatingSystemMXBean
import io.grpc.StatusRuntimeException
import link.rdcn.ErrorCode.USER_NOT_LOGGED_IN
import link.rdcn.dftree.OperationNode
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{BigIntVector, VarBinaryVector, VarCharVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.types.pojo.Schema
import link.rdcn.{ConfigLoader, Logging, SimpleSerializer}
import link.rdcn.provider.{DataProvider, DataStreamSource}
import link.rdcn.server.exception.{AuthorizationException, DataFrameAccessDeniedException, DataFrameNotFoundException}
import link.rdcn.struct.{DataFrame, Row, StructType, ValueType}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials}
import link.rdcn.util.DataUtils
import link.rdcn.user.DataOperationType
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, DataOperationType}
import link.rdcn.util.DataUtils.convertStructTypeToArrowSchema
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import scala.collection.convert.ImplicitConversions.`iterator asJava`

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 18:36
 * @Modified By:
 */

class FairdServer(dataProvider: DataProvider, authProvider: AuthProvider, fairdHome: String) {

  // 状态管理
  @volatile private var allocator: BufferAllocator = _
  @volatile private var producer: FlightProducerImpl = _
  @volatile private var flightServer: FlightServer = _
  @volatile private var serverThread: Thread = _
  @volatile private var started: Boolean = false

  private def buildServer(): Unit = {
    // 初始化配置
    ConfigLoader.init(s"$fairdHome/conf/faird.conf")
    val location = Location.forGrpcInsecure(
      ConfigLoader.fairdConfig.hostPosition,
      ConfigLoader.fairdConfig.hostPort
    )
    allocator = new RootAllocator()
    producer = new FlightProducerImpl(allocator, location, dataProvider, authProvider)
    flightServer = FlightServer.builder(allocator, location, producer).build()
  }

  def start(): Unit = synchronized {
    if (started) return

    buildServer()

    serverThread = new Thread(() => {
      try {
        flightServer.start()
        started = true
        Runtime.getRuntime.addShutdownHook(new Thread(() => {
          close()
        }))
        flightServer.awaitTermination()
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        started = false
      }
    })

    serverThread.setDaemon(false)
    serverThread.start()
  }

  def close(): Unit = synchronized {
    if (!started) return

    try {
      if (flightServer != null) flightServer.close()
    } catch {
      case _: Throwable => // ignore
    }

    try {
      if (allocator != null) allocator.close()
    } catch {
      case _: Throwable => // ignore
    }

    if (serverThread != null && serverThread.isAlive) {
      serverThread.interrupt()
    }

    // reset
    flightServer = null
    producer = null
    allocator = null
    serverThread = null
    started = false
  }

  def isStarted: Boolean = started
}

class FlightProducerImpl(allocator: BufferAllocator, location: Location, dataProvider: DataProvider, authProvider: AuthProvider) extends NoOpFlightProducer with Logging {

  private val requestMap = new ConcurrentHashMap[FlightDescriptor, (String, OperationNode)]()
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
              val dfName = root.getFieldVectors.get(0).asInstanceOf[VarCharVector].getObject(0).toString
              val userToken = root.getFieldVectors.get(1).asInstanceOf[VarCharVector].getObject(0).toString
              val authenticatedUser = Option(authenticatedUserMap.get(userToken))
              if(authenticatedUser.isEmpty){
                throw new AuthorizationException(USER_NOT_LOGGED_IN)
              }
              if(! authProvider.checkPermission(authenticatedUser.get, dfName, List.empty[DataOperationType].asJava.asInstanceOf[java.util.List[DataOperationType]] ))
                throw new DataFrameAccessDeniedException(dfName)
              val operationNodeJsonString = root.getFieldVectors.get(2).asInstanceOf[VarCharVector].getObject(0).toString
              val operationNode: OperationNode = OperationNode.fromJsonString(operationNodeJsonString)
              requestMap.put(flightStream.getDescriptor, (dfName, operationNode))
              flightStream.getRoot.clear()
            }
          }
        }
        ackStream.onCompleted()
      }
    }
  }

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
        new String(ticket.getBytes, StandardCharsets.UTF_8) match {
          case "listDataSetNames" => getListStringStream(dataProvider.listDataSetNames().asScala, listener)
          case ticketKey if ticketKey.startsWith("listDataFrameNames") => {
            val dataSet = ticketKey.replace("listDataFrameNames.","")
            getListStringStream(dataProvider.listDataFrameNames(dataSet).asScala, listener)
          }
          case ticketKey if ticketKey.startsWith("getSchemaURI") => {
            val dfName = ticketKey.replace("getSchemaURI.","")
            getSingleStringStream(dataProvider.getDataFrameSchemaURL(dfName),listener)

          }
          case ticketKey if ticketKey.startsWith("getDataSetMetaData") => {
            val dsName = ticketKey.replace("getDataSetMetaData.","")
            val model: Model = ModelFactory.createDefaultModel()
            dataProvider.getDataSetMetaData(dsName, model)
            getSingleStringStream(model.toString,listener)
          }
          case ticketKey if ticketKey.startsWith("getDataFrameSize") => {
            val dfName =  ticketKey.replace("getSchema.","")
            val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dfName)
            getSingleLongStream(dataStreamSource.rowCount, listener)
          }

          case ticketKey if ticketKey.startsWith("getHostInfo") =>
            val hostInfo =
              s"""
                 |faird.hostName: ${ConfigLoader.fairdConfig.hostName}
                 |faird.hostTitle: ${ConfigLoader.fairdConfig.hostTitle}
                 |faird.hostPosition: ${ConfigLoader.fairdConfig.hostPosition}
                 |faird.hostDomain: ${ConfigLoader.fairdConfig.hostDomain}
                 |faird.hostPort: ${ConfigLoader.fairdConfig.hostPort}
                 |""".stripMargin
            getSingleStringStream(hostInfo,listener)

          case ticketKey if ticketKey.startsWith("getServerResourceInfo") =>
            getSingleStringStream(getResourceStatusString,listener)

          case ticketKey if ticketKey.startsWith("getSchema") => {
            val dfName =  ticketKey.replace("getSchema.","")
            var structType = dataProvider.getDataFrameSchema(dfName)
            if(structType.isEmpty()){
              val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dfName)
              val iter = dataStreamSource.iterator
              if(iter.hasNext){
                 structType = DataUtils.inferSchemaFromRow(iter.next())
              }
            }
            getSingleStringStream(structType.toString,listener)
          }

          case _ => {
            val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))
            val request = requestMap.get(flightDescriptor)

            val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(request._1)
            val dataFrame = DataFrame(dataStreamSource.schema, dataStreamSource.iterator)
            val stream: Iterator[Row] = request._2.execute(dataFrame)
            val firstRow: Row = if(stream.hasNext) stream.next() else Row.empty
            val structType = if(firstRow.isEmpty) DataUtils.inferSchemaFromRow(firstRow) else StructType.empty
            val schema = convertStructTypeToArrowSchema(structType)

            //能否支持并发
            val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
            val root = VectorSchemaRoot.create(schema, childAllocator)
            val loader = new VectorLoader(root)
            listener.start(root)

            val arrowFlightStreamWriter = ArrowFlightStreamWriter(DataFrame(structType, Seq(firstRow).iterator ++ stream))
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
              requestMap.remove(flightDescriptor)
            }
          }
    }

  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {

      val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
      val request = requestMap.getOrDefault(descriptor, null)
      val schema =  if (request != null) {
        val dataFrameSchema = dataProvider.getDataFrameSchema(request._1)
        if (dataFrameSchema == StructType.empty) {
          throw new DataFrameNotFoundException(request._1)
        }
        else
          convertStructTypeToArrowSchema(dataProvider.getDataFrameSchema(request._1))

      } else new Schema(List.empty.asJava)
      new FlightInfo(schema, descriptor, List(flightEndpoint).asJava, -1L, 0L)
  }

  override def listFlights(context: FlightProducer.CallContext, criteria: Criteria, listener: FlightProducer.StreamListener[FlightInfo]): Unit = {
    requestMap.forEach {
      (k, v) => listener.onNext(getFlightInfo(null, k))
    }
    listener.onCompleted()
  }

  private def getResourceStatusString(): String = {
    val osBean = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[OperatingSystemMXBean]
    val runtime = Runtime.getRuntime

    val cpuLoadPercent = (osBean.getSystemCpuLoad * 100).formatted("%.2f")
    val availableProcessors = osBean.getAvailableProcessors

    val totalMemory = runtime.totalMemory() / 1024 / 1024 // MB
    val freeMemory = runtime.freeMemory() / 1024 / 1024   // MB
    val maxMemory = runtime.maxMemory() / 1024 / 1024     // MB
    val usedMemory = totalMemory - freeMemory

    val systemMemoryTotal = osBean.getTotalPhysicalMemorySize / 1024 / 1024 // MB
    val systemMemoryFree = osBean.getFreePhysicalMemorySize / 1024 / 1024   // MB
    val systemMemoryUsed = systemMemoryTotal - systemMemoryFree

    s"""
       |服务器资源使用情况:
       |-------------------------
       |CPU核心数         : $availableProcessors
       |CPU使用率         : $cpuLoadPercent%
       |
       |JVM内存 (MB):
       |  - 最大可用内存    : $maxMemory MB
       |  - 已分配内存      : $totalMemory MB
       |  - 已使用内存      : $usedMemory MB
       |  - 空闲内存        : $freeMemory MB
       |
       |系统物理内存 (MB):
       |  - 总内存          : $systemMemoryTotal MB
       |  - 已使用          : $systemMemoryUsed MB
       |  - 空闲            : $systemMemoryFree MB
       |-------------------------
       |""".stripMargin
  }

  private def getRootByStructType(structType: StructType): (VectorSchemaRoot, BufferAllocator) = {
    val schema = convertStructTypeToArrowSchema(structType)
    val childAllocator: BufferAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(schema, childAllocator)
    (root, childAllocator)
  }

  private def getSingleLongStream(long: Long, listener: FlightProducer.ServerStreamListener): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("size", ValueType.LongType))
    try {
      val nameVector = rootAndAllocator._1.getVector("size").asInstanceOf[BigIntVector]
      rootAndAllocator._1.allocateNew()
      nameVector.setSafe(0, long)
      rootAndAllocator._1.setRowCount(1)
      listener.start(rootAndAllocator._1)
      listener.putNext()
      listener.completed()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }

  private def getListStringStream(seq: Seq[String], listener: FlightProducer.ServerStreamListener): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("name", ValueType.StringType))
    try {
      val nameVector = rootAndAllocator._1.getVector("name").asInstanceOf[VarCharVector]
      rootAndAllocator._1.allocateNew()
      var index = 0
      seq.foreach(d => {
        nameVector.setSafe(index, d.getBytes("UTF-8"))
        index += 1
      })
      rootAndAllocator._1.setRowCount(index)
      listener.start(rootAndAllocator._1)
      listener.putNext()
      listener.completed()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }

  private def getSingleStringStream(str: String, listener: FlightProducer.ServerStreamListener): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("name", ValueType.StringType))
    try {
      val nameVector = rootAndAllocator._1.getVector("name").asInstanceOf[VarCharVector]
      rootAndAllocator._1.allocateNew()
      nameVector.setSafe(0, str.getBytes("UTF-8"))
      rootAndAllocator._1.setRowCount(1)
      listener.start(rootAndAllocator._1)
      listener.putNext()
      listener.completed()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }

}
