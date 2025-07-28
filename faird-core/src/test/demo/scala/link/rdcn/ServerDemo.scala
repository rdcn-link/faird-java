/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 10:49
 * @Modified By:
 */
package link.rdcn

import link.rdcn.server.FairdServer

import java.nio.file.Paths


object ServerDemo {
  def main(args: Array[String]): Unit = {
    val provider = new TestProvider
    //根据fairdHome自动读取配置文件
    val server = new FairdServer(provider.dataProvider, provider.authProvider, Paths.get(provider.getResourcePath("tls")).toString())
    server.start()
  }
}
