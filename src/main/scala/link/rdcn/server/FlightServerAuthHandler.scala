/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/17 15:57
 * @Modified By:
 */
package link.rdcn.server

import link.rdcn.ErrorCode.INVALID_CREDENTIALS
import link.rdcn.SimpleSerializer
import link.rdcn.server.exception.AuthorizationException
import link.rdcn.user.{Credentials, UsernamePassword}
import org.apache.arrow.flight.auth.ServerAuthHandler

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.{Optional, UUID}

object UserSessionStore {
  val users: Map[String, String] = Map("admin@instdb.cn" -> "admin001", "anoymous" -> "anoymous", "admin" -> "admin")
  val activeSessions: ConcurrentHashMap[String, Credentials] = new ConcurrentHashMap[String, Credentials]() // token -> username

  def validateUser(usernamePassword: UsernamePassword): Option[String] = {
    val userName = usernamePassword.username
    val password = usernamePassword.password
    users.get(userName) match {
      case _ => //假设任何时候都通过认证
        val token = UUID.randomUUID().toString // 简单生成一个 UUID 作为令牌
        activeSessions.put(token, usernamePassword)
        Some(token)
    }
  }

  def anonymousUser(credentials: Credentials): String = {
    val token = UUID.randomUUID().toString // 简单生成一个 UUID 作为令牌
    activeSessions.put(token, credentials)
    token
  }

  def isValidSession(token: String): Option[String] = {
    Option("")
  }
}

class FlightServerAuthHandler extends ServerAuthHandler{
  def authenticateCredentials(incomingCredentials: Credentials): Option[String] = {
    if (incomingCredentials.isInstanceOf[UsernamePassword]) {
        val usernamePassword = incomingCredentials.asInstanceOf[UsernamePassword]
        UserSessionStore.validateUser(usernamePassword) match {
          case Some(token) =>
//            println(s"Server: User $userName authenticated successfully. Issuing token.")
            // 认证成功，返回一个包含用户身份和令牌的字符串。这通常是一个 JWT 或类似的令牌，但在这里我们使用简单的 UUID 作为示例。
            // token 将发送回客户端
            Some(token)
          case None =>
//            println(s"Server: Authentication failed for user $userName.")
            throw new AuthorizationException(INVALID_CREDENTIALS)
        }
      } else if (incomingCredentials == Credentials.ANONYMOUS) {
            Some(UserSessionStore.anonymousUser(incomingCredentials))
    } else{
        throw new AuthorizationException(INVALID_CREDENTIALS)
      }
  }
  override def isValid(bytes: Array[Byte]): Optional[String] = {
//    println(s"Server: Validating session for token $bytes")
    Optional.ofNullable(UserSessionStore.isValidSession(new String(bytes)).getOrElse(null))
  }

  override def authenticate(serverAuthSender: ServerAuthHandler.ServerAuthSender, iterator: util.Iterator[Array[Byte]]): Boolean = {
    val credentialsBytes = iterator.next()
    authenticateCredentials(SimpleSerializer.deserialize(credentialsBytes).asInstanceOf[Credentials]) match {
      case Some(token) =>
        // 发送认证结果给客户端，其中包括 Bearer Token
        serverAuthSender.send(token.getBytes)
        true
      case _ => false
    }
  }
}
