/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 10:49
 * @Modified By:
 */
package link.rdcn

import link.rdcn.server.FairdServer


object ServerDemo {
  def main(args: Array[String]): Unit = {
    val provider = new TestProvider
    //根据fairdHome自动读取配置文件
    val server = new FairdServer(provider.dataProvider, provider.authProvider, provider.getResourcePath("tls"))
    server.start()
  }
}
