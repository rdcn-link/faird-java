package link.rdcn.dftp

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/28 15:10
 * @Modified By:
 */
trait DftpConfig {
  def host: String
  def port: Int
  def useTls: Boolean
  def tlsCertFile: String
  def tlsKeyFile: String
}
