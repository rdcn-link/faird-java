package org.grapheco

import org.junit.jupiter.api.Test
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext


/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/17 13:39
 * @Modified By:
 */
class FairdConfigTest {

  @Test
  def m1(): Unit = {
    val context: ApplicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");
    val config: FairdConfig = context.getBean("fairdConfig", classOf[FairdConfig])

    println("Host: " + config.getHostName());
    println("Title: " + config.getHostTitle());
    println("Domain: " + config.getHostDomain());
    println("Log Path: " + config.getLogPath());
  }
}
