package link.rdcn.user

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 16:55
 * @Modified By:
 */
trait Credentials extends Serializable {
  def isAnonymous: Boolean = false
}

object Credentials {
  case object Anonymous extends Credentials {
    override def isAnonymous: Boolean = true
  }
}
