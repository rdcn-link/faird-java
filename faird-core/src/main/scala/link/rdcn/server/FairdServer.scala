package link.rdcn.server

import com.sun.management.OperatingSystemMXBean
import io.circe.generic.auto._
import io.circe.parser._
import link.rdcn.ConfigKeys._
import link.rdcn.ErrorCode.{INVALID_CREDENTIALS, USER_NOT_LOGGED_IN}
import link.rdcn.dftree.Operation
import link.rdcn.provider.{DataProvider, DataStreamSource}
import link.rdcn.server.exception.{AuthorizationException, DataFrameAccessDeniedException, DataFrameNotFoundException}
import link.rdcn.struct.{DataFrame, LocalDataFrame, StructType, ValueType}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, DataOperationType, UsernamePassword}
import link.rdcn.util.DataUtils
import link.rdcn.util.DataUtils.convertStructTypeToArrowSchema
import link.rdcn.{ConfigLoader, Logging, SimpleSerializer}
import org.apache.arrow.flight._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.io.{File, StringWriter}
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
    ConfigLoader.init(fairdHome)
    print(ConfigLoader.fairdConfig.useTLS)
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
        .authHandler(new FlightServerAuthHandler())
        .build()
    }else{
      flightServer = FlightServer.builder(allocator, location, producer)
        .authHandler(new FlightServerAuthHandler())
        .build()
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

  private val requestMap = new ConcurrentHashMap[String, (String, Operation)]()
  private val authenticatedUserMap = new ConcurrentHashMap[String, AuthenticatedUser]()
  private val batchLen = 100

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
    val body = action.getBody
    action.getType match {
      case "listDataSetNames" =>
        getListStringStream(dataProvider.listDataSetNames().asScala, listener)
      case actionType if actionType.startsWith("listDataFrameNames") => {
        val dataSet = actionType.replace("listDataFrameNames.","")
        getListStringStream(dataProvider.listDataFrameNames(dataSet).asScala, listener)
      }
      case actionType if actionType.startsWith("getDataSetMetaData") => {
        val dsName = actionType.replace("getDataSetMetaData.","")
        val model: Model = ModelFactory.createDefaultModel
        dataProvider.getDataSetMetaData(dsName, model)
        val out: StringWriter  = new StringWriter()
        model.write(out, "TTL")
        getSingleStringStream(out.toString,listener)
      }
      case actionType if actionType.startsWith("getDataFrameSize") => {
        val dfName =  actionType.replace("getDataFrameSize.","")
        val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dfName)
        getSingleLongBytesStream(dataStreamSource.rowCount, listener)
      }

      case actionType if actionType.startsWith("getHostInfo") =>
        val hostInfo =
          s"""
             {"$FAIRD_HOST_NAME": "${ConfigLoader.fairdConfig.hostName}",
             "$FAIRD_HOST_TITLE": "${ConfigLoader.fairdConfig.hostTitle}",
             "$FAIRD_HOST_POSITION": "${ConfigLoader.fairdConfig.hostPosition}",
             "$FAIRD_HOST_DOMAIN": "${ConfigLoader.fairdConfig.hostDomain}",
             "$FAIRD_HOST_PORT": "${ConfigLoader.fairdConfig.hostPort}",
             "$FAIRD_TLS_ENABLED": "${ConfigLoader.fairdConfig.useTLS}",
             "$FAIRD_TLS_CERT_PATH": "${ConfigLoader.fairdConfig.certPath}",
             "$FAIRD_TLS_KEY_PATH": "${ConfigLoader.fairdConfig.keyPath}",
             "$LOGGING_FILE_NAME": "${ConfigLoader.fairdConfig.loggingFileName}",
             "$LOGGING_LEVEL_ROOT": "${ConfigLoader.fairdConfig.loggingLevelRoot}",
             "$LOGGING_PATTERN_CONSOLE": "${ConfigLoader.fairdConfig.loggingPatternConsole}",
             "$LOGGING_PATTERN_FILE": "${ConfigLoader.fairdConfig.loggingPatternFile}"
             }""".stripMargin.replaceAll("\n", "").replaceAll("\\s+", " ")
        getSingleStringStream(hostInfo,listener)

      case actionType if actionType.startsWith("getServerResourceInfo") =>
        getSingleStringStream(getResourceStatusString,listener)
      case actionType if actionType.startsWith("getDocument") => {
        val dfName = actionType.replace("getDocument.","")
        val dataFrameDocumentBytes = SimpleSerializer.serialize(dataProvider.getDocument(dfName))
        getArrayBytesStream(dataFrameDocumentBytes,listener)
      }
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
        getSingleStringStream(structType.toString,listener)
      }
      case actionType if actionType.startsWith("getStatistics") => {
        val dfName =  actionType.replace("getStatistics.","")
        val dataFrameStatisticsBytes = SimpleSerializer.serialize(dataProvider.getStatistics(dfName))
        getArrayBytesStream(dataFrameStatisticsBytes,listener)
      }
      case actionType if actionType.startsWith("login") =>
          val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
          val root = DataUtils.getVectorSchemaRootFromBytes(body,childAllocator)
          val credentialsJsonString: String = root.getFieldVectors.get(0).asInstanceOf[VarCharVector].getObject(0).toString
          val credentials = decode[UsernamePassword](credentialsJsonString)
          credentials match {
            case Left(_) => throw new AuthorizationException(INVALID_CREDENTIALS)
            case Right(usernamePassword: UsernamePassword) =>
              val authenticatedUser: AuthenticatedUser = authProvider.authenticate(usernamePassword)
              val loginToken: String = actionType.split("\\.")(1)
              authenticatedUserMap.put(loginToken, authenticatedUser)
          }
          listener.onCompleted()
      case actionType if actionType.startsWith("putRequest") =>
        val dfName =  actionType.split(":")(1)
        val userToken =  actionType.split(":")(2)
        val authenticatedUser = Option(authenticatedUserMap.get(userToken))
        if(authenticatedUser.isEmpty){
          throw new AuthorizationException(USER_NOT_LOGGED_IN)
        }
        if(! authProvider.checkPermission(authenticatedUser.get, dfName, List.empty[DataOperationType].asJava.asInstanceOf[java.util.List[DataOperationType]] ))
          throw new DataFrameAccessDeniedException(dfName)
        val operationNode: Operation = Operation.fromJsonString(new String(body, StandardCharsets.UTF_8))
        requestMap.put(userToken, (dfName, operationNode))
        listener.onCompleted()
      case _ =>
        throw new UnsupportedOperationException("Unsupported action type")
    }
  }

  override def getStream(context: FlightProducer.CallContext, ticket: Ticket, listener: FlightProducer.ServerStreamListener): Unit = {
      val userToken = new String(ticket.getBytes, StandardCharsets.UTF_8)
      new String(ticket.getBytes, StandardCharsets.UTF_8) match {
      case _ => {
        val request = requestMap.get(userToken)

        val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(request._1)
        val inDataFrame = LocalDataFrame(dataStreamSource.schema, dataStreamSource.iterator)

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
          requestMap.remove(userToken)
        }
      }
    }

  }

  override def getFlightInfo(context: FlightProducer.CallContext, descriptor: FlightDescriptor): FlightInfo = {
    val flightEndpoint = new FlightEndpoint(new Ticket(descriptor.getPath.get(0).getBytes(StandardCharsets.UTF_8)), location)
    val userToken = descriptor.getPath.get(0)
    val request = requestMap.getOrDefault(userToken, null)
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
      (k, v) => listener.onNext(getFlightInfo(null, FlightDescriptor.path(k)))
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
       |   {
       |    "cpu.cores"        : "$availableProcessors",
       |    "cpu.usage.percent" : "$cpuLoadPercent%",
       |
       |    "jvm.memory.max.mb" : "$maxMemory MB",
       |    "jvm.memory.total.mb" : "$totalMemory MB",
       |    "jvm.memory.used.mb" : "$usedMemory MB",
       |    "jvm.memory.free.mb" : "$freeMemory MB",
       |
       |    "system.memory.total.mb" : "$systemMemoryTotal MB",
       |    "system.memory.used.mb" : "$systemMemoryUsed MB",
       |    "system.memory.free.mb" : "$systemMemoryFree MB"
       |
       |}
       |""".stripMargin.stripMargin.replaceAll("\n", "").replaceAll("\\s+", " ")
  }

  private def getRootByStructType(structType: StructType): (VectorSchemaRoot, BufferAllocator) = {
    val schema = convertStructTypeToArrowSchema(structType)
    val childAllocator: BufferAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(schema, childAllocator)
    (root, childAllocator)
  }

  private def getSingleLongBytesStream(long: Long, listener: FlightProducer.StreamListener[Result]): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("rowCount", ValueType.LongType))
    try {
      val nameVector = rootAndAllocator._1.getVector("rowCount").asInstanceOf[BigIntVector]
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

  private def getListStringStream(seq: Seq[String], listener: FlightProducer.StreamListener[Result]): Unit = {
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

  private def getSingleStringStream(str: String, listener: FlightProducer.StreamListener[Result]): Unit = {
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

  private def getArrayBytesStream(bytes: Array[Byte], listener: FlightProducer.StreamListener[Result]): Unit = {
    val rootAndAllocator = getRootByStructType(StructType.empty.add("name", ValueType.BinaryType))
    try {
      val nameVector = rootAndAllocator._1.getVector("name").asInstanceOf[VarBinaryVector]
      rootAndAllocator._1.allocateNew()
      nameVector.setSafe(0, bytes)
      rootAndAllocator._1.setRowCount(1)
      listener.onNext(new Result(DataUtils.getBytesFromVectorSchemaRoot(rootAndAllocator._1)))
      listener.onCompleted()
    } finally {
      rootAndAllocator._1.close()
      rootAndAllocator._2.close()
    }
  }
}
