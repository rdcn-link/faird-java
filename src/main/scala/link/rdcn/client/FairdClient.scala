package link.rdcn.client

import link.rdcn.user.{Credentials, UsernamePassword}

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 14:49
 * @Modified By:
 */
private case class DacpUri(host: String, port: Int)

private object DacpUriParser {
  private val DacpPattern = "^dacp://([^:/]+):(\\d+)$".r

  def parse(uri: String): Either[String, DacpUri] = {
    uri match {
      case DacpPattern(host, portStr) =>
        try {
          val port = portStr.toInt
          if (port < 0 || port > 65535)
            Left(s"Invalid port number: $port")
          else
            Right(DacpUri(host, port))
        } catch {
          case _: NumberFormatException => Left(s"Invalid port format: $portStr")
        }

      case _ => Left(s"Invalid dacp URI format: $uri")
    }
  }
}

class FairdClient private (
                            url: String,
                            port: Int,
                            credentials: Credentials = Credentials.ANONYMOUS
                          ) {
  private val protocolClient = new ArrowFlightProtocolClient(url, port)
  protocolClient.login(credentials)

  def open(dataFrameName: String): RemoteDataFrameImpl =
    RemoteDataFrameImpl(dataFrameName, List.empty, protocolClient)

  def listDataSetNames(): Seq[String] =
    protocolClient.listDataSetNames()

  def listDataFrameNames(dsName: String): Seq[String] =
    protocolClient.listDataFrameNames(dsName)

  def getDataSetMetaData(dsName: String): String =
    protocolClient.getDataSetMetaData(dsName)

  def close(): Unit = protocolClient.close()
}


object FairdClient {

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): FairdClient =
    DacpUriParser.parse(url) match {
      case Right(parsed) =>
        new FairdClient(parsed.host, parsed.port, credentials)
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
}
