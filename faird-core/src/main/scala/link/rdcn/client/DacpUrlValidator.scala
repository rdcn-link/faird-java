package link.rdcn.client

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/14 15:34
 * @Modified By:
 */
object DacpUrlValidator {
  // Basic DFTP URL pattern: dftp://host:port/path
  private val DftpUrlPattern = "^dacp://([^:/]+)(?::(\\d+))?(/.*)?$".r

  def validate(url: String): Either[String, (String, Option[Int], String)] = {
    url match {
      case DftpUrlPattern(host, portStr, path) =>
        val port = Option(portStr).map { p =>
          try {
            val portNum = p.toInt
            if (portNum < 0 || portNum > 65535) {
              return Left(s"Invalid port number: $portNum")
            }
            portNum
          } catch {
            case _: NumberFormatException => return Left(s"Invalid port format: $p")
          }
        }

        val normalizedPath = Option(path).getOrElse("/")
        Right((host, port, normalizedPath))

      case _ =>
        Left(s"Invalid dacp URL format: $url. Expected format: dacp://host[:port][/path]")
    }
  }

  def validateWithPathPrefix(url: String, requiredPrefix: String): Either[String, (String, Option[Int], String)] = {
    validate(url).flatMap { case (host, port, path) =>
      if (path.startsWith(requiredPrefix)) {
        Right((host, port, path))
      } else {
        Left(s"DACP URL path must start with '$requiredPrefix'. Actual path: $path")
      }
    }
  }

  def validateAndExtractParam(url: String, prefix: String): Either[String, (String, Option[Int], String)] = {
    validateWithPathPrefix(url, prefix).flatMap { case (host, port, path) =>
      val param = path.stripPrefix(prefix)
      if (param.isEmpty) {
        Left(s"Missing parameter after '$prefix' in DACP URL")
      } else {
        Right((host, port, param))
      }
    }
  }

  /**
   * Quick validation check (boolean result)
   */
  def isValid(url: String): Boolean = validate(url).isRight

  /**
   * Extracts just the path component from a DFTP URL
   */
  def extractPath(url: String): Either[String, String] = {
    validate(url).map { case (_, _, path) => path }
  }
}
