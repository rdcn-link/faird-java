package link.rdcn

import link.rdcn.client.FairdClient
import link.rdcn.user.{Credentials, UsernamePassword}
import link.rdcn.util.ExceptionHandler
import link.rdcn.TestEmptyProvider.{adminPassword, adminUsername}
import org.apache.arrow.flight.FlightRuntimeException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

class ClientAuthorizationTest extends TestBase {

  @Test()
  def testLoginWhenUsernameIsNotAdmin(): Unit = {
    // 模拟非admin用户的情况进行测试
    val ServerException = assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", UsernamePassword("NotAdmin", adminPassword))
    )

    assertEquals(ErrorCode.USER_NOT_FOUND, ExceptionHandler.getErrorCode(ServerException))
  }

  @Test()
  def testInvalidCredentials(): Unit = {
    val ServerException = assertThrows(
      classOf[FlightRuntimeException],
      () => FairdClient.connect("dacp://0.0.0.0:3101", UsernamePassword(adminUsername, "wrongPassword"))
    )

    assertEquals(ErrorCode.INVALID_CREDENTIALS, ExceptionHandler.getErrorCode(ServerException))
  }

  //匿名访问DataFrame失败
  @Test
  def testAnonymousAccessDataFrameFalse(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101",Credentials.ANONYMOUS)
    val serverException = assertThrows(
      classOf[FlightRuntimeException],
      () => dc.open("/csv/data_1.csv").foreach(_ => ())
    )
    assertEquals(ErrorCode.USER_NOT_LOGGED_IN, ExceptionHandler.getErrorCode(serverException))
  }

}
