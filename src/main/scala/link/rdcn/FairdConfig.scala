package link.rdcn

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 17:23
 * @Modified By:
 */
case class FairdConfig(
                        hostName: String,
                        hostTitle: String,
                        hostPosition: String,
                        hostDomain: String,
                        hostPort: Int,
                        useTLS: Boolean,
                        certPath: String,
                        keyPath: String,
                        logggingFileName: String,
                        logggingLevelRoot: String,
                        loggingPatternConsole: String,
                        loggingPatternFile: String
                      )

object FairdConfig {

  /** 从 java.util.Properties 加载配置生成 FairdConfig 实例 */
  def load(props: java.util.Properties): FairdConfig = {
    def getOrDefault(key: String, default: String): String =
      Option(props.getProperty(key))
        .getOrElse(default)

    FairdConfig(
      hostName = getOrDefault("faird.host.name", ""),
      hostTitle = getOrDefault("faird.host.title", ""),
      hostPosition = getOrDefault("faird.host.position","0.0.0.0"),
      hostDomain = getOrDefault("faird.host.domain",""),
      hostPort = getOrDefault("faird.host.port","3101").toInt,
      useTLS = getOrDefault("faird.tls.enabled", "false").toBoolean,
      certPath = getOrDefault("faird.tls.cert.path","server.crt"),
      keyPath = getOrDefault("faird.tls.key.path,", "server.pem"),
      logggingFileName = getOrDefault("logging.file.name","./access.log"),
      logggingLevelRoot = getOrDefault("logging.level.root","INFO"),
      loggingPatternConsole = getOrDefault("logging.pattern.console","%d{HH:mm:ss} %-5level %logger{36} - %msg%n"),
      loggingPatternFile = getOrDefault("logging.pattern.file","%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger - %msg%n")
    )
  }
}