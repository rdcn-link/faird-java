package link.rdcn.user

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 16:55
 * @Modified By:
 */
trait Credentials extends Serializable

object Credentials {
  case object ANONYMOUS extends Credentials

  def anonymous: Credentials = ANONYMOUS
}

