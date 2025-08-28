/**
 * @Author Yomi
 * @Description:
 * @Data 2025/8/28 15:49
 * @Modified By:
 */
package link.rdcn.struct

import org.junit.jupiter.api.Test

import java.io.File

class DataStreamSourceFactoryTest {
  @Test
  def testServerNotRunning(): Unit = {
    val jsonSource =  DataStreamSourceFactory.createJSONDataStreamSource(new File(
      "C:\\Users\\ASUS\\Documents\\Projects\\PycharmProjects\\Faird\\Faird\\faird-core\\src\\test\\demo\\data\\json\\million_lines.json")
      ,false)
    jsonSource.iterator.next()
  }

}
