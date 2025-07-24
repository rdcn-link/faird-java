/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/24 18:08
 * @Modified By:
 */
package link.rdcn

import link.rdcn.TestEmptyProvider.getResourcePath
import link.rdcn.client.FairdClient
import link.rdcn.client.dag._
import link.rdcn.struct.DataFrame
import link.rdcn.user.UsernamePassword

import java.nio.file.Paths

object ClienMultiLanguageCodeDemo {

  def main(args: Array[String]): Unit = {
    // 连接Faird服务
    val dc: FairdClient = FairdClient.connectTLS("dacp://localhost:3101", UsernamePassword("admin@instdb.cn", "admin001"))

    // 使用自定义的java代码对数据帧进行操作
    val code =
      """
        |import java.util.*;
        |
        |import link.rdcn.client.dag.UDFFunction;
        |import link.rdcn.struct.Row;
        |
        |public class DynamicUDF implements UDFFunction {
        |    public DynamicUDF() {
        |        // 默认构造器，必须显式写出
        |    }
        |    @Override
        |    public link.rdcn.struct.DataFrame transform(final link.rdcn.struct.DataFrame dataFrame) {
        |        return dataFrame.filter(row -> {
        |                    long value = (long) row._1();
        |                    return value <= 3;
        |       });
        |    }
        |}
        |""".stripMargin

    //构建数据源节点
    val sourceNode: FlowNode = SourceNode("/csv/data_1.csv")
    //构建java代码操作节点
    val javaCodeNode = JavaCodeNode(code, "DynamicUDF")
    val transformerDAGJavaCode: Flow = Flow.pipe(sourceNode, javaCodeNode)
    val javaDAGDfs: Seq[DataFrame] = dc.execute(transformerDAGJavaCode)
    println("--------------打印通过自定义Java代码操作的数据帧--------------")
    javaDAGDfs.foreach(df => df.limit(3).foreach(row => println(row)))

    // 使用指定依赖的自定义python操作对数据帧进行操作
    ConfigLoader.init(getResourcePath(""))
    val whlPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "link-0.1-py3-none-any.whl").toString
    val pythonWhlFunction = PythonWhlFunctionNode("id1", "normalize", whlPath)
    val transformerDAGPythonWhl: Flow = Flow.pipe(sourceNode, pythonWhlFunction)
    val pythonBinDAGDfs: Seq[DataFrame] = dc.execute(transformerDAGPythonWhl)
    println("--------------打印通过自定义Java代码操作的数据帧--------------")
    pythonBinDAGDfs.foreach(df => df.limit(3).foreach(row => println(row)))

  }

}
