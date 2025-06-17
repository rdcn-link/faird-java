package org.grapheco

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/17 15:44
 * @Modified By:
 */
trait AuthProvider {
  /** 用户认证，返回认证后的用户信息（包含权限）或失败 */
  def authenticate(credentials: Credentials): Either[AuthError, AuthenticatedUser]

  /** 判断用户是否具有某项权限 */
  def authorize(user: AuthenticatedUser, action: AuthAction): Boolean
}

// 用户凭据（可根据不同实现扩展，如密码、Token等）
sealed trait Credentials
case class UsernamePassword(username: String, password: String) extends Credentials
case class TokenAuth(token: String) extends Credentials

// 表示经过认证的用户信息
case class AuthenticatedUser(
                              userId: String,
                              roles: Set[String],
                              permissions: Set[String] // 可选：直接列出权限
                            )

// 授权行为的抽象定义
case class AuthAction(resource: String, action: String) // 例如：("document:123", "read")

// 错误信息
sealed trait AuthError
case object InvalidCredentials extends AuthError
case object UserNotFound extends AuthError
case object TokenExpired extends AuthError
