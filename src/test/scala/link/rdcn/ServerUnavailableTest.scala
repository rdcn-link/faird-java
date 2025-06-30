package link.rdcn

import link.rdcn.TestBase._
import link.rdcn.client.FairdClient
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.{BeforeEach, Test}


class ServerUnavailableTest {

//  @BeforeEach
//  def closeServer(): Unit = {
//    flightServer.close()
//  }

  //服务未启动
  @Test
  def testServerNotRunning(): Unit = {

    val exception = assertThrows(
      classOf[Exception],
      () => FairdClient.connect("dacp://0.0.0.0:3101", adminUsername, adminPassword)

    )
    assertEquals("server is not running!", exception.getMessage)
  }

}
