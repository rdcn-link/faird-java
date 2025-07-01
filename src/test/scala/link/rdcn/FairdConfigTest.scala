package link.rdcn

import link.rdcn.ConfigLoader.{initLog4j, loadFairdConfig, loadProperties}
import link.rdcn.FairdConfigTest.getResourcePath
import org.apache.logging.log4j.{LogManager, Logger}
import org.junit.jupiter.api.Test


/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/17 13:39
 * @Modified By:
 */
object FairdConfigTest  {
  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName)))
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    url.getPath
  }
}

class FairdConfigTest {

  @Test
  def m1(): Unit = {
    ConfigLoader.init(getResourcePath("faird.conf"))
    val config = ConfigBridge.getConfig


    val logger: Logger = LogManager.getLogger(getClass)

    logger.info("日志已初始化")
    logger.info(s"主机: ${config.getHostName}, 标题: ${config.getHostTitle}, 端口: ${config.getHostPort}")

    println("Host: " + config.getHostName());
    println("Title: " + config.getHostTitle());
    println("Domain: " + config.getHostDomain());
  }
}
