/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/11 16:09
 * @Modified By:
 */
package link.rdcn.util.FlightAuthHandlerDemo

import org.apache.arrow.flight._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.flight.auth.ClientAuthHandler
import org.apache.arrow.flight.CallHeaders
import org.apache.arrow.flight.auth.ClientAuthHandler

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.util
import scala.collection.JavaConverters._

// --- 客户端认证处理器实现 ---
class MyClientAuthHandler(username: String, password: String) extends ClientAuthHandler {
  private var sessionToken: Option[String] = None

  // authenticate 方法用于初始握手认证 (客户端发送凭证，接收服务器返回的令牌)
  // 对应 Flight 文档中 "ClientAuthHandler.authenticate" 部分
  // 从服务器的响应头中获取 Bearer Token
  // 注意: Flight 框架会自动处理从 ServerAuthHandler 返回的令牌。
  // 在这里，incomingCredentials 通常不会直接包含 "Bearer" 头，因为 Flight 内部会处理令牌的交换。
  // `ClientAuthHandler.authenticate` 的返回值才是传递给 `getNextCallHeaders` 的 token。
  // 但是，为了演示目的，我们可以假装从 `incomingCredentials` 中读取一个自定义的令牌。
  // 实际上，更常见的模式是：服务器在 `AuthResult` 中返回令牌，然后 Flight 框架将其传递给客户端的 `AuthHandler`。
  // 对于 BearerTokenAuthenticator，服务器返回的 `AuthResult.info` 就是令牌。
  // 这里我们简单地通过 `client.authenticate` 的返回值来获取令牌。

  override def authenticate(clientAuthSender: ClientAuthHandler.ClientAuthSender, iterator: util.Iterator[Array[Byte]]): Unit = {
    clientAuthSender.send(s"Basic $username:$password".getBytes)

    val tokenBytes = iterator.next() // 假设服务器返回的第一个字节数组就是令牌
    val token = new String(tokenBytes, StandardCharsets.UTF_8)
    setSessionToken(token)
    println("Client: Received server response during authentication.")
  }

  override def getCallToken: Array[Byte] = sessionToken.getOrElse("").getBytes(StandardCharsets.UTF_8)

  // 设置会话令牌的方法 (在 authenticate 成功后由外部调用)
  def setSessionToken(token: String): Unit = {
    sessionToken = Some(token)
  }
}


// --- Flight 客户端主类 ---
object MyFlightClient {
  def main(args: Array[String]): Unit = {
    MyFlightServer.start()
    val allocator = new RootAllocator(Long.MaxValue)

    //    val trustedCertFile = new File("path/to/ca.crt")
    //
    //    if (!trustedCertFile.exists()) {
    //      println("Error: Trusted certificate file not found. Please provide a valid path.")
    //      return
    //    }

    val clientBuilder = FlightClient.builder(
      allocator,
      Location.forGrpcInsecure("localhost", 50050),
    )

    val client = clientBuilder.build()
    val username = "dremio"
    val password = "dremio123"
    val clientAuthHandler = new MyClientAuthHandler(username, password)
    println("Attempting to authenticate with FlightClient.authenticate()...")
    // **触发认证握手**。这个调用会调用 clientAuthHandler.authenticate()
    client.authenticate(clientAuthHandler)

    // 客户端请求响应时服务端从客户端AuthHandler拿到token并检查是否有效
    client.doAction(new Action("my_action", "some_payload".getBytes)).hasNext
    MyFlightServer.close()
  }
}