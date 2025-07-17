/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/17 15:53
 * @Modified By:
 */
package link.rdcn.client

import link.rdcn.SimpleSerializer
import link.rdcn.user.Credentials
import org.apache.arrow.flight.auth.ClientAuthHandler

import java.nio.charset.StandardCharsets
import java.util

class FlightClientAuthHandler(credentials: Credentials) extends ClientAuthHandler  {
  private var sessionToken: Option[String] = None

  override def authenticate(clientAuthSender: ClientAuthHandler.ClientAuthSender, iterator: util.Iterator[Array[Byte]]): Unit = {
    clientAuthSender.send(SimpleSerializer.serialize(credentials))

    val tokenBytes = iterator.next() // 假设服务器返回的第一个字节数组就是令牌
    val token = new String(tokenBytes, StandardCharsets.UTF_8)
    setSessionToken(token)
    println("Client: Received server response during authentication.")
  }

  override def getCallToken: Array[Byte] = sessionToken.getOrElse("").getBytes(StandardCharsets.UTF_8)

  def getSessionToken: String  = {
    sessionToken.getOrElse(null)
  }

  // 设置会话令牌的方法 (在 authenticate 成功后由外部调用)
  def setSessionToken(token: String): Unit = {
    sessionToken = Some(token)
  }

}
