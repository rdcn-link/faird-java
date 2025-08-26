package link.rdcn.server.dftp

import link.rdcn.client.UrlValidator
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, DataOperationType}
import link.rdcn.util.ServerUtils
import link.rdcn.{ConfigLoader, FairdConfig}
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import java.io.File
import java.nio.file.Paths
import java.util
import java.util.concurrent.ConcurrentHashMap

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 14:31
 * @Modified By:
 */
class DftpServer {
  val url = s"${protocolSchema}://${ConfigLoader.fairdConfig.hostPosition}:${ConfigLoader.fairdConfig.hostPort}"
  val urlValidator = UrlValidator(protocolSchema)
  def setProtocolSchema(protocolSchema: String): Unit = this.protocolSchema = protocolSchema

  def setAuthHandler(authProvider: AuthProvider): Unit = this.authProvider = authProvider

  def setDftpServiceHandler(dftpServiceHandler: DftpServiceHandler): Unit = this.dftpServiceHandler = dftpServiceHandler

  def getDftpServiceHandler(): DftpServiceHandler = this.dftpServiceHandler

  final def get(request: GetRequest, response: GetResponse): Unit = {
    val url = request.getRequestedUrl()
    urlValidator.validate(url) match {
      case Left(value) => response.sendError(400, s"bad request $value")
      case Right(value) => dftpServiceHandler.doGet(request, response)
    }
  }

  private val authenticatedUserMap = new ConcurrentHashMap[String, AuthenticatedUser]()

  private var authProvider: AuthProvider = new AuthProvider {
    override def authenticate(credentials: Credentials): AuthenticatedUser = new AuthenticatedUser{
      override def token: String = "token"
    }
    override def checkPermission(user: AuthenticatedUser, dataFrameName: String, opList: util.List[DataOperationType]): Boolean = true
  }

  private var dftpServiceHandler: DftpServiceHandler = new DftpServiceHandler {

    override def doGet(request: GetRequest, response: GetResponse): Unit = {
      response.sendError(404, s"resource ${request.getRequestedUrl()} not found")
    }

    override def doPut(request: PutRequest, putResponse: PutResponse): Unit = {
      putResponse.sendError(204, s"No Content")
    }

    override def doAction(request: ActionRequest, response: ActionResponse): Unit = {
      response.sendError(404, s"Action ${request.getActionName()} not found")
    }

  }

  private var protocolSchema: String = "dftp"

  @volatile private var allocator: BufferAllocator = _
  @volatile private var flightServer: FlightServer = _
  @volatile private var serverThread: Thread = _
  @volatile private var started: Boolean = false

  private def buildServer(fairdConfig: FairdConfig): Unit = {
    // 初始化配置
    ConfigLoader.init(fairdConfig)
    val location = if(ConfigLoader.fairdConfig.useTLS)
      Location.forGrpcTls(ConfigLoader.fairdConfig.hostPosition, ConfigLoader.fairdConfig.hostPort)
    else
      Location.forGrpcInsecure(ConfigLoader.fairdConfig.hostPosition, ConfigLoader.fairdConfig.hostPort)

    allocator = new RootAllocator()
    ServerUtils.init(allocator)

    val producer = new DftpFlightProducer(allocator, location, this)

    if(ConfigLoader.fairdConfig.useTLS){
      flightServer = FlightServer.builder(allocator, location, producer)
        .useTls(new File(Paths.get(fairdConfig.fairdHome, ConfigLoader.fairdConfig.certPath).toString), new File(Paths.get(fairdConfig.fairdHome, ConfigLoader.fairdConfig.keyPath).toString))
        .authHandler(new FlightServerAuthHandler(authProvider, authenticatedUserMap))
        .build()
    }else{
      flightServer = FlightServer.builder(allocator, location, producer)
        .authHandler(new FlightServerAuthHandler(authProvider, authenticatedUserMap))
        .build()
    }

  }

  def start(fairdConfig: FairdConfig): Unit = synchronized {
    if (started) return

    buildServer(fairdConfig)

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
    allocator = null
    serverThread = null
    started = false
  }

  def isStarted: Boolean = started

}
