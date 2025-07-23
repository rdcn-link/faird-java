package link.rdcn

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder
/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/23 17:10
 * @Modified By:
 */

object ConfigLoader {

  private var props: Properties = _
  var fairdConfig: FairdConfig = _

  def init(fairdHome: String): Unit = synchronized {
    props = loadProperties(s"$fairdHome"+File.separator+"conf"+File.separator+"faird.conf")
    props.setProperty(ConfigKeys.FAIRD_HOME, fairdHome)
    fairdConfig = FairdConfig.load(props)
    initLog4j(props)
  }

  private def loadProperties(path: String): Properties = {
    val props = new Properties()
    val fis = new InputStreamReader(new FileInputStream(path), "UTF-8")
    try props.load(fis) finally fis.close()
    props
  }

  private def initLog4j(props: Properties): Unit = {
    val builder: ConfigurationBuilder[BuiltConfiguration] = ConfigurationBuilderFactory.newConfigurationBuilder()

    builder.setStatusLevel(Level.WARN)
    builder.setConfigurationName("FairdLogConfig")

    val logFile = props.getProperty("logging.file.name", "./default.log")
    val level = Level.toLevel(props.getProperty("logging.level.root", "INFO"))
    val consolePattern = props.getProperty("logging.pattern.console", "%d{HH:mm:ss} %-5level %logger{36} - %msg%n")
    val filePattern = props.getProperty("logging.pattern.file", "%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger - %msg%n")

    val console = builder.newAppender("Console", "CONSOLE")
      .add(builder.newLayout("PatternLayout").addAttribute("pattern", consolePattern))
    builder.add(console)

    val file = builder.newAppender("File", "FILE")
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

object ConfigBridge{
  def getConfig(): FairdConfig = ConfigLoader.fairdConfig
}

