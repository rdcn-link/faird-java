/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/28 18:45
 * @Modified By:
 */
package link.rdcn.client

import link.rdcn.client.dag._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class FlowNodeTest {
  @Test
  def testSource(): Unit = {
    assertEquals(SourceNode("/csv/data_1.csv"), FlowNode.source("/csv/data_1.csv"))
  }

  @Test
  def testFromJavaClass(): Unit = {
    assertEquals(JavaCodeNode("javaCode", "className"), FlowNode.fromJavaClass("className", "javaCode"))
  }

  @Test
  def FromRepository(): Unit = {
    assertEquals(PythonWhlFunctionNode("id","name","whlPath"), FlowNode.fromRepository("id","name","whlPath"))
  }

  @Test
  def testFromBin(): Unit = {
    assertEquals(BinNode("id","name","path"), FlowNode.fromBin("id","name","path"))

  }

}
