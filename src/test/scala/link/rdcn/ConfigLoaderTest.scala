package link.rdcn

import link.rdcn.ConfigLoader.{initLog4j, loadFairdConfig, loadProperties}
import link.rdcn.ConfigLoaderTest.getResourcePath
import link.rdcn.util.ExpectedConfigLoader
import link.rdcn.util.SharedValue.{allocator, configCache, location, producer}
import link.rdcn.util.ExpectedConfigLoader
import org.apache.arrow.flight.{FlightRuntimeException, FlightServer, Location}
import org.apache.logging.log4j.{LogManager, Logger}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test


/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/17 13:39
 * @Modified By:
 */
object ConfigLoaderTest {
  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName)))
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    url.getPath
  }
}

class ConfigLoaderTest {

  //是否加载配置文件
  @Test
  def initTest(): Unit = {
    val configPath = getResourcePath("/conf/faird.conf")
    ConfigLoader.fairdConfig = configCache
    ConfigLoader.init(configPath)
    val config = ConfigBridge.getConfig

    assertEquals(ExpectedConfigLoader.getHostName, config.getHostName)
    assertEquals(ExpectedConfigLoader.getHostTitle, config.getHostTitle)
    assertEquals(ExpectedConfigLoader.getHostPort, config.getHostPort)
    assertEquals(ExpectedConfigLoader.getHostDomain, config.getHostDomain)
    assertEquals(ExpectedConfigLoader.getHostPosition, config.getHostPosition)
    assertEquals(ExpectedConfigLoader.getCatdbPort, config.getCatdbPort)
  }


  //未加载Config
  @Test
  def testConfigNotInit(): Unit = {
    ConfigLoader.fairdConfig = null

    assertThrows(
      classOf[NullPointerException],
      () => Location.forGrpcInsecure(ConfigLoader.fairdConfig.getHostPosition,
        ConfigLoader.fairdConfig.getHostPort)
    )

  }

}
