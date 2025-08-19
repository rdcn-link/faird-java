package link.rdcn.client

import link.rdcn.user.{Credentials, UsernamePassword}
import link.rdcn.util.DataUtils
import org.apache.arrow.flight.auth.ClientAuthHandler

import java.nio.charset.StandardCharsets
import java.util

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/17 15:53
 * @Modified By:
 */
class FlightClientAuthHandler(credentials: Credentials) extends ClientAuthHandler  {

  private var callToken: Array[Byte] = _

  override def authenticate(clientAuthSender: ClientAuthHandler.ClientAuthSender, iterator: util.Iterator[Array[Byte]]): Unit = {
    credentials match {
      case UsernamePassword(username, password) => clientAuthSender.send(DataUtils.codeUserPassword(username, password))
      case Credentials.ANONYMOUS =>
        clientAuthSender.send(DataUtils.codeUserPassword("ANONYMOUS","ANONYMOUS"))
      case _ => new IllegalArgumentException(s"$credentials not supported")
    }
    try {
      callToken = iterator.next()
    }catch {
      case _: Exception => callToken = null
    }

  }

  override def getCallToken: Array[Byte] = callToken

}
