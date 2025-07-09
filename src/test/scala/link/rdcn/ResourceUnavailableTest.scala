package link.rdcn

import link.rdcn.TestBase.{csvDir, dc, getServer, stopServer}
import link.rdcn.util.ExceptionHandler
import org.apache.arrow.flight.FlightRuntimeException
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

class ResourceUnavailableTest extends TestBase {

  //df不存在
  @Test
  def testAccessInvalidDataFrame(): Unit = {
    val df = dc.open("/csv/invalid.csv") // 假设没有该文件
    val serverException = assertThrows(
      classOf[FlightRuntimeException],
      () => df.foreach(_ => {})
    )
    assertEquals(ErrorCode.DATAFRAME_NOT_EXIST, ExceptionHandler.getErrorCode(serverException))
  }
}
