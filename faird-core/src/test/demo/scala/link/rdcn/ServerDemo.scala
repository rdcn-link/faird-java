/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 10:49
 * @Modified By:
 */
package link.rdcn

import link.rdcn.TestBase.getResourcePath
import link.rdcn.server.FairdServer

import java.nio.file.Paths


object ServerDemo {
  def main(args: Array[String]): Unit = {
    val provider = new TestDemoProvider
    //根据fairdHome自动读取配置文件
    val server = new FairdServer(provider.dataProvider, provider.authProvider, Paths.get(getResourcePath("tls")).toString())
//    val server = new FairdServer(provider.dataProvider, provider.authProvider, Paths.get(getResourcePath("")).toString())
    server.start()
  }
}
