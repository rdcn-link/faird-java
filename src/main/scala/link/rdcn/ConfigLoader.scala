package link.rdcn

import java.io.FileInputStream
import java.util.Properties

import org.apache.logging.log4j.{Level, LogManager, Logger}
import org.apache.logging.log4j.core.config.builder.api._
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory
import org.apache.logging.log4j.core.config.Configurator

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/23 17:10
 * @Modified By:
 */

object ConfigLoader {

  val props = loadProperties()
  val fairdConfig =  loadFairdConfig(props)
  initLog4j(props)

  def loadProperties(): Properties = {
    val props = new Properties()
    val configFilePath = getClass.getClassLoader.getResource("faird.conf").getPath
    val fis = new java.io.InputStreamReader(new java.io.FileInputStream(configFilePath), "UTF-8")
    try props.load(fis) finally fis.close()
    props
  }

  def loadFairdConfig(props: Properties): FairdConfig = {
    val config = new FairdConfig
    config.setHostName(props.getProperty("faird.hostName"))
    config.setHostTitle(props.getProperty("faird.hostTitle"))
    config.setHostPosition(props.getProperty("faird.hostPosition"))
    config.setHostDomain(props.getProperty("faird.hostDomain"))
    config.setHostPort(props.getProperty("faird.hostPort").toInt)
    config.setCatdbPort(props.getProperty("faird.catdbPort").toInt)
    config
  }

  def initLog4j(props: Properties): Unit = {
    val builder: ConfigurationBuilder[BuiltConfiguration] = ConfigurationBuilderFactory.newConfigurationBuilder()

    builder.setStatusLevel(Level.WARN)
    builder.setConfigurationName("FairdLogConfig")

    val logFile = props.getProperty("logging.file.name", "./default.log")
    val level = Level.toLevel(props.getProperty("logging.level.root", "INFO"))
    val consolePattern = props.getProperty("logging.pattern.console", "%d{HH:mm:ss} %-5level %logger{36} - %msg%n")
    val filePattern = props.getProperty("logging.pattern.file", "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger - %msg%n")

    val console = builder.newAppender("Console", "CONSOLE")
      .addAttribute("charset", "UTF-8")
      .add(builder.newLayout("PatternLayout").addAttribute("pattern", consolePattern))
    builder.add(console)

    val file = builder.newAppender("File", "FILE")
      .addAttribute("charset", "UTF-8")
      .addAttribute("fileName", logFile)
      .add(builder.newLayout("PatternLayout").addAttribute("pattern", filePattern))
    builder.add(file)

    builder.add(
      builder.newRootLogger(level)
        .add(builder.newAppenderRef("Console"))
        .add(builder.newAppenderRef("File"))
    )

    Configurator.initialize(builder.build())
  }
}

