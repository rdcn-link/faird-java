package link.rdcn.dftree

import link.rdcn.{ConfigLoader, SimpleSerializer}
import link.rdcn.TestBase.getResourcePath
import link.rdcn.dftree.FunctionWrapper.{CppBin, JavaCode, JavaJar, PythonBin}
import link.rdcn.struct.ValueType.IntType
import link.rdcn.struct._
import link.rdcn.util.ClosableIterator
import org.codehaus.janino.SimpleCompiler
import org.json.JSONObject
import org.junit.jupiter.api.Test

import java.nio.file.Paths
import java.util.Base64
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}
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

    val compiler = new SimpleCompiler()
    compiler.cook(code)
    val clazz = compiler.getBytecodes.asScala.toMap
    val jo = new JSONObject()
    jo.put("type", LangType.JAVA_CODE.name)
    jo.put("clazz", Base64.getEncoder.encodeToString(SimpleSerializer.serialize(new java.util.HashMap[String, Array[Byte]](clazz.asJava))))
    val javaCode = FunctionWrapper(jo).asInstanceOf[JavaCode]
    val rows = LocalDataFrame(StructType.empty.add("id",IntType).add("value",IntType),ClosableIterator(Seq(Row.fromSeq(Seq(1,2))).iterator)())
    val newDataFrame = javaCode.applyToInput(rows.stream).asInstanceOf[DataFrame]
    newDataFrame.foreach(row => {
      assert(row._1 == 1)
      assert(row._2 == 2)
      assert(row._3 == 100)
    })
  }

  @Test
  def pythonBinTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val whlPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "link-0.1-py3-none-any.whl").toString
    val jo = new JSONObject()
    jo.put("type", LangType.PYTHON_BIN.name)
    jo.put("functionID", "aaa.bbb.id1")
    jo.put("functionName", "normalize")
    jo.put("whlPath", whlPath)
    val pythonBin = FunctionWrapper(jo).asInstanceOf[PythonBin]
    val rows = Seq(Row.fromSeq(Seq(1,2))).iterator
    val jep = JepInterpreterManager.getJepInterpreter("id1", whlPath)
    val newRow = pythonBin.applyToInput(rows, Some(jep)).asInstanceOf[Iterator[Row]].next()
    assert(newRow._1 == 0.33)
    assert(newRow._2 == 0.67)
  }

  @Test
  def javaJarTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val jo = new JSONObject()
    jo.put("type", LangType.JAVA_JAR.name)
    jo.put("functionID", "aaa.bbb.id2")
    jo.put("fileName","faird-plugin-impl-1.0-20250707.jar")
    val javaJar = FunctionWrapper(jo).asInstanceOf[JavaJar]
    val rows = Seq(Row.fromSeq(Seq(1,2))).iterator
    val dataFrame = LocalDataFrame(StructType.empty.add("col_1", ValueType.IntType).add("col_2", ValueType.IntType), ClosableIterator(rows)())
    val newDataFrame = javaJar.applyToInput(dataFrame).asInstanceOf[DataFrame]
    newDataFrame.foreach(row => {
      assert(row._1 == 1)
      assert(row._2 == 2)
      assert(row._3 == 100)
    })
  }

  @Test
  def cppBinTest(): Unit = {
    ConfigLoader.init(getResourcePath(""))
    val cppPath = Paths.get(ConfigLoader.fairdConfig.fairdHome, "lib", "cpp", "cpp_processor.exe").toString
    val jo = new JSONObject()
    jo.put("type", LangType.CPP_BIN.name)
    jo.put("functionID", "cpp_processor.exe")
    val cppBin = FunctionWrapper(jo).asInstanceOf[CppBin]
    val rows = Seq(Row.fromSeq(Seq(1,2))).iterator
    val dataFrame = LocalDataFrame(StructType.empty.add("col_1", ValueType.IntType).add("col_2", ValueType.IntType), ClosableIterator(rows)())
    val newDf = cppBin.applyToInput(dataFrame).asInstanceOf[DataFrame]
    newDf.foreach(row => {
      assert(row._1 == true)
    })

  }
}

