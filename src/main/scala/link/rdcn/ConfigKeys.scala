package link.rdcn

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/16 14:11
 * @Modified By:
 */
object ConfigKeys {

  // === 自定义配置项 ===
  val FairdHostName       = "faird.host.name"
  val FairdHostTitle      = "faird.host.title"
  val FairdHostPosition   = "faird.host.position"
  val FairdHostDomain     = "faird.host.domain"
  val FairdHostPort       = "faird.host.port"

  // === 日志配置 ===
  val LoggingFileName     = "logging.file.name"
  val LoggingLevelRoot    = "logging.level.root"
  val LoggingPatternConsole = "logging.pattern.console"
  val LoggingPatternFile  = "logging.pattern.file"

  // === 网络安全配置 ===
  val FairdTlsEnabled     = "faird.tls.enabled"
  val FairdTlsCertPath    = "faird.tls.cert.path"
  val FairdTlsKeyPath     = "faird.tls.key.path"
}
