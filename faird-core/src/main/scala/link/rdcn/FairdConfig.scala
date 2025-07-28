package link.rdcn

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 17:23
 * @Modified By:
 */
case class FairdConfig(
                        fairdHome: String,
                        hostName: String,
                        hostTitle: String,
                        hostPosition: String,
                        hostDomain: String,
                        hostPort: Int,
                        useTLS: Boolean,
                        certPath: String,
                        keyPath: String,
                        loggingFileName: String,
                        loggingLevelRoot: String,
                        loggingPatternConsole: String,
                        loggingPatternFile: String,
                        pythonHome: String
                      )

object FairdConfig {

  /** 从 java.util.Properties 加载配置生成 FairdConfig 实例 */
  def load(props: java.util.Properties): FairdConfig = {
    def getOrDefault(key: String, default: String): String =
      Option(props.getProperty(key))
        .getOrElse(default)

    FairdConfig(
      fairdHome = getOrDefault(ConfigKeys.FAIRD_HOME, ""),
      hostName = getOrDefault(ConfigKeys.FAIRD_HOST_NAME, ""),
      hostTitle = getOrDefault(ConfigKeys.FAIRD_HOST_TITLE, ""),
      hostPosition = getOrDefault(ConfigKeys.FAIRD_HOST_POSITION,"0.0.0.0"),
      hostDomain = getOrDefault(ConfigKeys.FAIRD_HOST_DOMAIN,""),
      hostPort = getOrDefault(ConfigKeys.FAIRD_HOST_PORT,"3101").toInt,
      useTLS = getOrDefault(ConfigKeys.FAIRD_TLS_ENABLED, "false").toBoolean,
      certPath = getOrDefault(ConfigKeys.FAIRD_TLS_CERT_PATH, "server.crt"),
      keyPath = getOrDefault(ConfigKeys.FAIRD_TLS_KEY_PATH, "server.pem"),
      loggingFileName = getOrDefault(ConfigKeys.LOGGING_FILE_NAME,"./access.log"),
      loggingLevelRoot = getOrDefault(ConfigKeys.LOGGING_LEVEL_ROOT,"INFO"),
      loggingPatternConsole = getOrDefault(ConfigKeys.LOGGING_PATTERN_CONSOLE,"%d{HH:mm:ss} %-5level %logger{36} - %msg%n"),
      loggingPatternFile = getOrDefault(ConfigKeys.LOGGING_PATTERN_FILE,"%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger - %msg%n"),
      pythonHome = getOrDefault(ConfigKeys.PYTHON_HOME,null),
    )
  }
}