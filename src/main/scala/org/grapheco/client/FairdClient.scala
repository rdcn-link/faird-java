package org.grapheco.client

import org.grapheco.server.{RemoteDataFrameImpl, DacpClient}

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

object FairdClient{
  def connect (url: String): DacpClient = {
    DacpUriParser.parse(url) match {
      case Right(parsed) =>
        new DacpClient(parsed.host, parsed.port)
      case Left(err) =>
        throw new Exception(err)
    }
  }
  def connect(url: String, user: String, password: String): DacpClient = {
    DacpUriParser.parse(url) match {
      case Right(parsed) =>
        new DacpClient(parsed.host, parsed.port, user, password)
      case Left(err) =>
        throw new Exception(err)
    }
  }
}
