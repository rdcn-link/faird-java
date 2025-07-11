/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/11 16:20
 * @Modified By:
 */
package link.rdcn.util.FlightAuthHandlerDemo

import link.rdcn.ErrorCode._
import link.rdcn.server.exception.AuthorizationException
import org.apache.arrow.flight._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight.auth.ServerAuthHandler

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.{Optional, UUID}
import scala.collection.JavaConverters._

// --- 模拟用户存储和会话管理 ---
object UserSessionStore {
  val users: Map[String, String] = Map("dremio" -> "dremio123", "testuser" -> "password")
  val activeSessions: ConcurrentHashMap[String, String] = new ConcurrentHashMap[String, String]() // token -> username

  def validateUser(username: String, password: String): Option[String] = {
    users.get(username) match {
      case Some(p) if p == password =>
        val token = UUID.randomUUID().toString // 简单生成一个 UUID 作为令牌
        activeSessions.put(token, username)
        Some(token)
      case _ => None
    }
  }

  def isValidSession(token: String): Option[String] = {
    Option(activeSessions.get(token))
  }
}

// --- 服务器端认证处理实现 ---
class MyServerAuthHandler extends ServerAuthHandler {

  // InitialAuthenticator 处理初始的握手认证 (客户端发送凭证，服务器验证)
  // 对应 Flight 文档中 "ServerAuthHandler.authenticate" 部分
  def authenticateCredentials(incomingCredentials: String): Option[String] = {
    if (incomingCredentials != null && incomingCredentials.startsWith("Basic ")) {
      val credentials = incomingCredentials.substring("Basic ".length)
      val parts = credentials.split(":", 2)
      if (parts.length == 2) {
        val username = parts(0)
        val password = parts(1)
        UserSessionStore.validateUser(username, password) match {
          case Some(token) =>
            println(s"Server: User $username authenticated successfully. Issuing token.")
            // 认证成功，返回一个包含用户身份和令牌的字符串。这通常是一个 JWT 或类似的令牌，但在这里我们使用简单的 UUID 作为示例。
            // token 将通过 Flight 机制发送回客户端
            Some(token)
          case None =>
            println(s"Server: Authentication failed for user $username.")
            throw new AuthorizationException(INVALID_CREDENTIALS)
        }
      } else {
        throw new AuthorizationException(INVALID_CREDENTIALS)
      }
    } else {
      throw new AuthorizationException(INVALID_CREDENTIALS)
    }
  }


  // 客户端进行请求时验证Token是否有效
  override def isValid(bytes: Array[Byte]): Optional[String] = {
    println(s"Server: Validating session for token $bytes")
    Optional.ofNullable(UserSessionStore.isValidSession(new String(bytes)).getOrElse(null))
  }

  override def authenticate(serverAuthSender: ServerAuthHandler.ServerAuthSender, iterator: util.Iterator[Array[Byte]]): Boolean = {
    val credentialsBytes = iterator.next()
    val credentials = new String(credentialsBytes, StandardCharsets.UTF_8)
    authenticateCredentials(credentials) match {
      case Some(token) =>
        // 发送认证结果给客户端，其中包括 Bearer Token
        serverAuthSender.send(token.getBytes)
        true
      case _ => false
    }
  }
}

// --- Flight Producer 实现 ---
class MyFlightProducer extends NoOpFlightProducer {
  // 注意: 当使用了 ServerAuthHandler 后，Flight 框架会在调用以下方法前自动处理认证。


  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
    // 认证请求由 MyServerAuthHandler 处理，此处无需额外处理认证逻辑
    println(s"Server doAction for user: ${context.peerIdentity()}, type: ${action.getType}")
    val body = action.getBody
    action.getType match {
      case _ =>
        listener.onNext(new Result(s"Processed action ${action.getType}".getBytes))
        listener.onCompleted()
    }
  }
}

// --- 服务器启动入口 ---
object MyFlightServer {
  val allocator = new RootAllocator(Long.MaxValue)
  val serverBuilder = FlightServer.builder(
      allocator,
      Location.forGrpcInsecure("0.0.0.0", 50050),
      new MyFlightProducer()
    )
    .authHandler(new MyServerAuthHandler()) // **注册 ServerAuthHandler**
  val server = serverBuilder.build()

  def start(): Unit = {


    println(s"MyFlightServer with TLS and ServerAuthHandler started on ${server.getLocation}")
    server.start()
  }

  def close(): Unit = {
    server.shutdown()
  }

}