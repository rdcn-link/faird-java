package link.rdcn.user


/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/1 15:04
 * @Modified By:
 */
case class UsernamePassword(userName: String, password: String) extends Credentials {
  override def isAnonymous: Boolean = this == Credentials.ANONYMOUS
}