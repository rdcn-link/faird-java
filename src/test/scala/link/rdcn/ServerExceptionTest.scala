package link.rdcn

import link.rdcn.client.FairdClient
import link.rdcn.server.FairdServer
import link.rdcn.user.UsernamePassword
import link.rdcn.util.ExceptionHandler
import link.rdcn.util.SharedValue._
import org.apache.arrow.flight.{FlightRuntimeException, FlightServer}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

import java.io.IOException


class ServerExceptionTest {

  //服务未启动
  @Test
  def testServerNotRunning(): Unit = {
    val exception = assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", UsernamePassword(adminUsername, adminPassword))

    )
    assertEquals(ErrorCode.SERVER_NOT_RUNNING, ExceptionHandler.getErrorCode(exception))
  }

  //端口被占用
  @Test()
  def testAddressAlreadyInUse(): Unit = {
    val flightServer1 = FlightServer.builder(allocator, location, producer).build()
    flightServer1.start()
    val flightServer2 = FlightServer.builder(allocator, location, producer).build()
    val ServerException = assertThrows(
      classOf[IOException],
      () => flightServer2.start()
    )
    flightServer1.close()
    assertEquals(ErrorCode.SERVER_ADDRESS_ALREADY_IN_USE, ExceptionHandler.getErrorCode(ServerException))
  }

  //服务重复启动
  @Test()
  def testServerAlreadyStarted(): Unit = {
    val flightServer: FlightServer = FlightServer.builder(allocator, location, producer).build()
    flightServer.start()
    val ServerException = assertThrows(
      classOf[IllegalStateException],
      () => flightServer.start()
    )
    flightServer.close()
    assertEquals(ErrorCode.SERVER_ALREADY_STARTED, ExceptionHandler.getErrorCode(ServerException))
  }

  //schema不匹配
  //访问时断开连接 server closed
  //url错误（端口和ip）

}
