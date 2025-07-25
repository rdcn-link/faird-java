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
    val javaCode =
      """
        |import java.util.*;
        |import link.rdcn.util.*;
        |import link.rdcn.client.dag.Transformer11;
        |import link.rdcn.struct.*;
        |
        |public class DynamicUDF implements Transformer11 {
        |    public DynamicUDF() {
        |        // 默认构造器，必须显式写出
        |    }
        |    @Override
        |    public link.rdcn.struct.DataFrame transform(final link.rdcn.struct.DataFrame dataFrame) {
        |            final scala.collection.Iterator<Row> iter = ((LocalDataFrame)dataFrame).stream();
        |            final scala.collection.Iterator<Row> rows =  new scala.collection.Iterator<Row>() {
        |            public boolean hasNext() {
        |                return iter.hasNext();
        |            }
        |            public Row next() {
        |                Row row = (Row)iter.next();
        |                return Row.fromJavaList(Arrays.asList(row.get(0), row.get(1), 100));
        |            }
        |        };
        |                return DataUtils.getDataFrameByStream(rows);
        |            }
        |}
        |""".stripMargin

    //构建数据源节点
    val sourceNode: FlowNode = SourceNode("/csv/data_1.csv")
    //构建java代码操作节点
    val javaCodeNode = JavaCodeNode(javaCode, "DynamicUDF")
    val transformerDAGJavaCode: Flow = Flow.pipe(sourceNode, javaCodeNode)
    val javaDAGDfs: Seq[DataFrame] = dc.execute(transformerDAGJavaCode)
    println("--------------打印通过自定义Java代码操作的数据帧--------------")
    javaDAGDfs.foreach(df => df.limit(3).foreach(row => println(row)))

    // 使用指定依赖的自定义python二进制文件对数据帧进行操作
    ConfigLoader.init(getResourcePath(""))
    val whlPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "link-0.1-py3-none-any.whl").toString
    val pythonWhlFunction = PythonWhlFunctionNode("id1", "normalize", whlPath)
    val transformerDAGPythonWhl: Flow = Flow.pipe(sourceNode, pythonWhlFunction)
    val pythonBinDAGDfs: Seq[DataFrame] = dc.execute(transformerDAGPythonWhl)
    println("--------------打印通过自定义Python代码操作的数据帧--------------")
    pythonBinDAGDfs.foreach(df => df.limit(3).foreach(row => println(row)))

    // 使用自定义二进制程序对数据帧进行操作（示意）
    val binPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "bin", "func.exe").toString
    val binFunction = BinNode("id2","func",binPath)
    val transformerDAGBin: Flow = Flow.pipe(sourceNode, binFunction)
    val binDAGDfs: Seq[DataFrame] = dc.execute(transformerDAGBin)
    println("--------------打印通过二进制程序对代码操作的数据帧--------------")
    binDAGDfs.foreach(df => df.limit(3).foreach(row => println(row)))

  }

}
