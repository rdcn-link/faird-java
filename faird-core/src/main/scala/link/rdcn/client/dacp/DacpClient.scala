package link.rdcn.client.dacp

import link.rdcn.client.{DftpClient, RemoteDataFrameProxy}
import link.rdcn.struct.DataFrame
import link.rdcn.user.Credentials
import link.rdcn.util.ClientUtils
import org.apache.arrow.flight.Action

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/18 16:30
 * @Modified By:
 */
class DacpClient(url: String, port: Int, useTLS: Boolean = false) extends DftpClient(url, port, useTLS) {

  override def get(url: String, body: Array[Byte]): DataFrame = {
    val path = DacpUrlValidator.extractPath(url) match {
      case Right(path) => path
      case Left(message) => throw new IllegalArgumentException(message)
    }
    path match {
      case actionType if actionType.startsWith("/get") =>
        val dataFrameName = actionType.stripPrefix("/get/")
        RemoteDataFrameProxy(dataFrameName, this)
      case _ =>
        val actionResult = flightClient.doAction(new Action(path))
        ClientUtils.parseFlightActionResults(actionResult)
    }
  }

  override def put(dataFrame: DataFrame): Unit = ???
}

object DacpClient {
  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClient = {
    DacpUrlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101))
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }


  def connectTLS(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClient = {
    DacpUrlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101), true)
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
