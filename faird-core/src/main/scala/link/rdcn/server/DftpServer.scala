package link.rdcn.server

import link.rdcn.ConfigLoader
import link.rdcn.util.ServerUtils
import link.rdcn.struct.{DataFrame}
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import java.io.File
import java.nio.file.Paths

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 14:31
 * @Modified By:
 */

sealed trait ActionType{
  def name: String
}
object ActionType {
  case object GET extends ActionType {
    override def name: String = "GET"
  }
  case object PUT extends ActionType {
    override def name: String = "PUT"
  }

  def fromString(s: String): Option[ActionType] = s.toUpperCase match {
    case "GET" => Some(GET)
    case "PUT" => Some(PUT)
    case _     => None
  }
}

/**
 * case 200 => resp.dataFrame.foreach(df => println(s"Got DataFrame with ${df.count()} rows"))
 * case 404 => println("Not found")
 * case 400 => println("Bad request")
 * case 500 => println(s"Server error: ${resp.message.getOrElse("")}")
 *
 * */
case class DftpRequest(path: String, actionType: ActionType, body: Array[Byte] = Array.emptyByteArray)
case class DftpResponse(var code: Int, var dataFrame: Option[DataFrame] = None, var message: Option[String] = None){
  def set(code: Int, dataFrame: Option[DataFrame] = None, message: Option[String] = None): Unit = {
    this.code = code
    this.dataFrame = dataFrame
    this.message = message
  }
}

abstract class DftpServer(fairdHome: String) {

  // 状态管理
  @volatile var allocator: BufferAllocator = _
  @volatile private var flightServer: FlightServer = _
  @volatile private var serverThread: Thread = _
  @volatile private var started: Boolean = false

  def doGet(request: DftpRequest, response: DftpResponse): Unit

  def doPut(request: DftpRequest, response: DftpResponse): Unit

  private def buildServer(): Unit = {
    // 初始化配置
    ConfigLoader.init(fairdHome)
    val location = if(ConfigLoader.fairdConfig.useTLS)
      Location.forGrpcTls(ConfigLoader.fairdConfig.hostPosition, ConfigLoader.fairdConfig.hostPort)
    else
      Location.forGrpcInsecure(ConfigLoader.fairdConfig.hostPosition, ConfigLoader.fairdConfig.hostPort)

    allocator = new RootAllocator()
    ServerUtils.init(allocator)

    val producer = new DftpFlightProducer(allocator, location, this)

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
    allocator = null
    serverThread = null
    started = false
  }

  def isStarted: Boolean = started

}
