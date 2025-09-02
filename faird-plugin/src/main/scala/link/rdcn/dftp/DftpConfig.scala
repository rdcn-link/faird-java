package link.rdcn.dftp

import java.io.File

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
  def tlsCertFile: File
  def tlsKeyFile: File
}
