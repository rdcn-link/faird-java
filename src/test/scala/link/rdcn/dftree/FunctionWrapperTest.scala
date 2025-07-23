package link.rdcn.dftree

import link.rdcn.ConfigLoader
import link.rdcn.dftree.FunctionWrapper.{JavaCode, PythonBin}
import link.rdcn.struct.Row
import org.json.JSONObject
import org.junit.jupiter.api.Test

import java.nio.file.Paths
/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/16 15:42
 * @Modified By:
 */
class FunctionWrapperTest {

  @Test
  def javaCodeTest(): Unit = {
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
        |    public scala.collection.Iterator<Row> transform(final scala.collection.Iterator<Row> iter) {
        |        final scala.collection.Iterator<Row> rows =  new scala.collection.Iterator<Row>() {
        |            public boolean hasNext() {
        |                return iter.hasNext();
        |            }
        |
        |            public Row next() {
        |                Row row = (Row)iter.next();
        |                return Row.fromJavaList(Arrays.asList(row.get(0), row.get(1), 100));
        |            }
        |        };
        |        return rows;
        |    }
        |}
        |""".stripMargin
    val jo = new JSONObject()
    jo.put("type", LangType.JAVA_CODE.name)
    jo.put("javaCode", code)
    jo.put("className", "DynamicUDF")
    val javaCode = FunctionWrapper(jo).asInstanceOf[JavaCode]
    val rows = Seq(Row.fromSeq(Seq(1,2))).iterator
    val newRow = javaCode.applyToInput(rows).asInstanceOf[Iterator[Row]].next()
    assert(newRow._3 == 100)
  }

  @Test
  def pythonBinTest(): Unit = {
    ConfigLoader.init(getClass.getClassLoader.getResource("").getPath)
    val whlPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "link-0.1-py3-none-any.whl").toString
    val jo = new JSONObject()
    jo.put("type", LangType.PYTHON_BIN.name)
    jo.put("functionId", "id1")
    jo.put("functionName", "normalize")
    jo.put("whlPath", whlPath)
    val pythonBin = FunctionWrapper(jo).asInstanceOf[PythonBin]
    val rows = Seq(Row.fromSeq(Seq(1,2))).iterator
    val jep = JepInterpreterManager.getJepInterpreter("id1", whlPath)
    val newRow = pythonBin.applyToInput(rows, Some(jep)).asInstanceOf[Iterator[Row]].next()
    assert(newRow._1 == 0.33)
    assert(newRow._2 == 0.67)
  }
}

