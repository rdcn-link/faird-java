package link.rdcn

import link.rdcn.ConfigLoader.{initLog4j, loadFairdConfig, loadProperties}
import org.apache.logging.log4j.{LogManager, Logger}
import org.junit.jupiter.api.Test


/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/17 13:39
 * @Modified By:
 */
class FairdConfigTest {

  @Test
  def m1(): Unit = {
    val props = loadProperties()

    val config = loadFairdConfig(props)
    initLog4j(props)

    val logger: Logger = LogManager.getLogger(getClass)

    logger.info("日志已初始化")
    logger.info(s"主机: ${config.getHostName}, 标题: ${config.getHostTitle}, 端口: ${config.getHostPort}")

    println("Host: " + config.getHostName());
    println("Title: " + config.getHostTitle());
    println("Domain: " + config.getHostDomain());
  }
}
