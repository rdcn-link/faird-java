package link.rdcn.server

import com.sun.management.OperatingSystemMXBean
import link.rdcn.ErrorCode.USER_NOT_LOGGED_IN
import link.rdcn.dftree.Operation
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
import link.rdcn.util.DataUtils.convertStructTypeToArrowSchema
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.LockSupport
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

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
    ConfigLoader.init(s"$fairdHome"+File.separator+"conf"+File.separator+"faird.conf")
    val location = if(ConfigLoader.fairdConfig.useTLS) Location.forGrpcTls(
      ConfigLoader.fairdConfig.hostPosition,
      ConfigLoader.fairdConfig.hostPort
    ) else Location.forGrpcInsecure(
      ConfigLoader.fairdConfig.hostPosition,
      ConfigLoader.fairdConfig.hostPort
    )
    allocator = new RootAllocator()
    producer = new FlightProducerImpl(allocator, location, dataProvider, authProvider)
    if(ConfigLoader.fairdConfig.useTLS){
      flightServer = FlightServer.builder(allocator, location, producer)
        .useTls(new File(Paths.get(fairdHome, ConfigLoader.fairdConfig.certPath).toString), new File(Paths.get(fairdHome, ConfigLoader.fairdConfig.keyPath).toString))
        .build()
    }else{
      flightServer = FlightServer.builder(allocator, location, producer).build()
    }

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

  private val requestMap = new ConcurrentHashMap[FlightDescriptor, (String, Operation)]()
  private val authenticatedUserMap = new ConcurrentHashMap[String, AuthenticatedUser]()
  private val batchLen = 100

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {

    new Runnable {
      override def run(): Unit = {
        val ticketKey: String = flightStream.getDescriptor.getPath.get(0)
        ticketKey match {
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
              val operationNode: Operation = Operation.fromJsonString(operationNodeJsonString)
              requestMap.put(flightStream.getDescriptor, (dfName, operationNode))
              flightStream.getRoot.clear()
            }
          }
        }
        ackStream.onCompleted()
      }
    }
  }

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
    val body = action.getBody
    action.getType match {
      case "listDataSetNames" =>
        getListBytesStream(dataProvider.listDataSetNames().asScala, listener)
      case actionType if actionType.startsWith("listDataFrameNames") => {
        val dataSet = actionType.replace("listDataFrameNames.","")
        getListBytesStream(dataProvider.listDataFrameNames(dataSet).asScala, listener)
      }
      case actionType if actionType.startsWith("getSchemaURI") => {
        val dfName = actionType.replace("getSchemaURI.","")
        getSingleBytesStream(dataProvider.getDataFrameDocument(dfName).getSchemaURL.getOrElse(""),listener)
      }
      case actionType if actionType.startsWith("getDataSetMetaData") => {
        val dsName = actionType.replace("getDataSetMetaData.","")
        val model: Model = ModelFactory.createDefaultModel()
        dataProvider.getDataSetMetaData(dsName, model)
        getSingleBytesStream(model.toString,listener)
      }
      case actionType if actionType.startsWith("getDataFrameSize") => {
        val dfName =  actionType.replace("getDataFrameSize.","")
        val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dfName)
        getSingleLongBytesStream(dataStreamSource.rowCount, listener)
      }

      case actionType if actionType.startsWith("getHostInfo") =>
        val hostInfo =
          s"""
             |faird.hostName: ${ConfigLoader.fairdConfig.hostName}
             |faird.hostTitle: ${ConfigLoader.fairdConfig.hostTitle}
             |faird.hostPosition: ${ConfigLoader.fairdConfig.hostPosition}
             |faird.hostDomain: ${ConfigLoader.fairdConfig.hostDomain}
             |faird.hostPort: ${ConfigLoader.fairdConfig.hostPort}
             |""".stripMargin
        getSingleBytesStream(hostInfo,listener)

      case actionType if actionType.startsWith("getServerResourceInfo") =>
        getSingleBytesStream(getResourceStatusString,listener)

      case actionType if actionType.startsWith("getSchema") => {
        val dfName =  actionType.replace("getSchema.","")
        val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dfName)
        var structType = dataStreamSource.schema
        if(structType.isEmpty()){
          val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dfName)
          val iter = dataStreamSource.iterator
          if(iter.hasNext){
            structType = DataUtils.inferSchemaFromRow(iter.next())
          }
        }
        getSingleBytesStream(structType.toString,listener)
      }
      case actionType if actionType.startsWith("login") =>
          val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
          val root = DataUtils.getVectorSchemaRootFromBytes(body,childAllocator)
          val credentialsBytes = root.getFieldVectors.get(0).asInstanceOf[VarBinaryVector].getObject(0)
          val credentials = SimpleSerializer.deserialize(credentialsBytes).asInstanceOf[Credentials]
          val authenticatedUser: AuthenticatedUser = authProvider.authenticate(credentials)
          val loginToken: String = actionType.split("\\.")(1)
          authenticatedUserMap.put(loginToken, authenticatedUser)
          listener.onCompleted()
      case _ =>
        throw new UnsupportedOperationException("Unsupported action type")
    }
  }

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
        new String(ticket.getBytes, StandardCharsets.UTF_8) match {
          case _ => {
            val flightDescriptor = FlightDescriptor.path(new String(ticket.getBytes, StandardCharsets.UTF_8))
            val request = requestMap.get(flightDescriptor)

            val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(request._1)
            val inDataFrame = DataFrame(dataStreamSource.schema, dataStreamSource.iterator)
            val outDataFrame: DataFrame  = request._2.execute(inDataFrame)
            val schema = convertStructTypeToArrowSchema(outDataFrame.schema)

            //能否支持并发
            val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
            val root = VectorSchemaRoot.create(schema, childAllocator)
            val loader = new VectorLoader(root)
            listener.start(root)

            val arrowFlightStreamWriter = ArrowFlightStreamWriter(outDataFrame)
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
      val structType = dataProvider.getDataStreamSource(request._1).schema
      val schema =  if (request != null) {
        val dataFrameSchema = structType
        if (dataFrameSchema == StructType.empty) {
          throw new DataFrameNotFoundException(request._1)
        }
        else
          convertStructTypeToArrowSchema(structType)

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

  private def getSingleLongBytesStream(long: Long, listener: FlightProducer.StreamListener[Result]): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("size", ValueType.LongType))
    try {
      val nameVector = rootAndAllocator._1.getVector("size").asInstanceOf[BigIntVector]
      rootAndAllocator._1.allocateNew()
      nameVector.setSafe(0, long)
      rootAndAllocator._1.setRowCount(1)
      listener.onNext(new Result(DataUtils.getBytesFromVectorSchemaRoot(rootAndAllocator._1)))
      listener.onCompleted()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }

  private def getListBytesStream(seq: Seq[String], listener: FlightProducer.StreamListener[Result]): Unit = {
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
      listener.onNext(new Result(DataUtils.getBytesFromVectorSchemaRoot(rootAndAllocator._1)))
      listener.onCompleted()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }

  private def getSingleBytesStream(str: String, listener: FlightProducer.StreamListener[Result]): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("name", ValueType.StringType))
    try {
      val nameVector = rootAndAllocator._1.getVector("name").asInstanceOf[VarCharVector]
      rootAndAllocator._1.allocateNew()
      nameVector.setSafe(0, str.getBytes("UTF-8"))
      rootAndAllocator._1.setRowCount(1)
      listener.onNext(new Result(DataUtils.getBytesFromVectorSchemaRoot(rootAndAllocator._1)))
      listener.onCompleted()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }
}
