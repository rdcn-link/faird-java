package link.rdcn.server

import link.rdcn.user.{AuthProvider, AuthenticatedUser, UsernamePassword}
import link.rdcn.util.DataUtils
import org.apache.arrow.flight.auth.ServerAuthHandler

import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.{Optional, UUID}

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/17 15:57
 * @Modified By:
 */

class FlightServerAuthHandler(authProvider: AuthProvider, tokenMap: ConcurrentHashMap[String, AuthenticatedUser]) extends ServerAuthHandler{

  override def authenticate(serverAuthSender: ServerAuthHandler.ServerAuthSender, iterator: util.Iterator[Array[Byte]]): Boolean = {
    try{
      val cred = DataUtils.decodeUserPassword(iterator.next())
      val authenticatedUser = authProvider.authenticate(UsernamePassword(cred._1, cred._2))
      val token = UUID.randomUUID().toString()
      tokenMap.put(token, authenticatedUser)
      serverAuthSender.send(token.getBytes(StandardCharsets.UTF_8))
      true
    }catch {
      case e: Exception => false
    }
  }

  override def isValid(bytes: Array[Byte]): Optional[String] = {
    val tokenStr = new String(bytes, StandardCharsets.UTF_8)
    Optional.of(tokenStr)
  }

}
