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

  var fairdConfig: FairdConfig = _

  def init(fairdHome: String): Unit = synchronized {
    val props = loadProperties(s"$fairdHome" + File.separator + "conf" + File.separator + "faird.conf")
    props.setProperty(ConfigKeys.FAIRD_HOME, fairdHome)
    fairdConfig = FairdConfig.load(props)
    initLog4j(fairdConfig)
  }

  def init(config: FairdConfig): Unit =
    if (config != null) fairdConfig = config else fairdConfig = new FairdConfig

  private def loadProperties(path: String): Properties = {
    val props = new Properties()
    val fis = new InputStreamReader(new FileInputStream(path), "UTF-8")
    try props.load(fis) finally fis.close()
    props
  }

  private def initLog4j(config: FairdConfig): Unit = {
    val builder: ConfigurationBuilder[BuiltConfiguration] = ConfigurationBuilderFactory.newConfigurationBuilder()

    builder.setStatusLevel(Level.WARN)
    builder.setConfigurationName("FairdLogConfig")

    val logFile = config.loggingFileName
    val level = Level.toLevel(config.loggingLevelRoot)
    val consolePattern = config.loggingPatternConsole
    val filePattern = config.loggingPatternFile

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

object ConfigBridge {
  def getConfig(): FairdConfig = ConfigLoader.fairdConfig
}

