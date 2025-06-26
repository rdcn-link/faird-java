package link.rdcn


import link.rdcn.client.{DacpClient, FairdClient}
import link.rdcn.provider.DataProvider
import link.rdcn.server.FlightProducerImpl
import link.rdcn.struct.ValueType.{DoubleType, IntType, LongType, StringType}
import link.rdcn.struct.{CSVSource, DataFrameInfo, DataSet, DirectorySource, Row, StructType}
import link.rdcn.util.DataUtils
import link.rdcn.util.DataUtils.listFiles
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.grapheco.TestDataGenerator
import org.grapheco.TestDataGenerator.getOutputDir
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.{PrintWriter, StringWriter}
import java.nio.file.{Files, Path, Paths}
import scala.util.Random
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.awt.Color
import scala.io.Source

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */

object ClientTest{
  val location = Location.forGrpcInsecure(ConfigLoader.fairdConfig.getHostPosition, ConfigLoader.fairdConfig.getHostPort)
  val allocator: BufferAllocator = new RootAllocator()

//  generalData
  TestDataGenerator.generateTestData()
  val csvDfInfos = listFiles(getOutputDir("test_output/csv").toString).map(file => {
    DataFrameInfo(file.getAbsolutePath, CSVSource(",", true), StructType.empty.add("id", IntType).add("value", DoubleType))
  })
  val binDfInfos = Seq(
    DataFrameInfo(getOutputDir("test_output/bin").toString, DirectorySource(false),StructType.binaryStructType))

  val dataSetCsv = DataSet("csv","1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin","2", binDfInfos.toList)

  val dataProvider = new DataProvider(){
    override val dataSets: List[DataSet] = List(dataSetCsv,dataSetBin)
  }


  val producer = new FlightProducerImpl(allocator, location, dataProvider)
  val flightServer = FlightServer.builder(allocator, location, producer).build()
  lazy val dc: DacpClient = FairdClient.connect("dacp://0.0.0.0:3101")
  @BeforeAll
  def startServer(): Unit = {
    //
    flightServer.start()
    println(s"Server (Location): Listening on port ${flightServer.getPort}")

  }
  @AfterAll
  def stopServer(): Unit = {
    //
    producer.close()
    flightServer.close()
//    dc.close()

    DataUtils.closeAllFileSources()
    TestDataGenerator.cleanupTestData()
  }


  def genModel: Model = {
    ModelFactory.createDefaultModel()
  }


}

class ClientTest {
  val provider = ClientTest.dataProvider
  val csvModel: Model = ClientTest.genModel
  val binModel: Model = ClientTest.genModel
  val dc: DacpClient = ClientTest.dc

  @Test
  def testListDataSetNames(): Unit = {
    assertEquals(provider.listDataSetNames().toSet,dc.listDataSetNames().toSet,"ListDataSetNames接口输出与预期不符！")
  }

  @Test
  def testListDataFrameNames(): Unit = {
    assertEquals(provider.listDataFrameNames("csv").toSet, dc.listDataFrameNames("csv").toSet,"ListDataFrameNames接口读取csv文件输出与预期不符！")
    assertEquals(provider.listDataFrameNames("bin").toSet, dc.listDataFrameNames("bin").toSet,"ListDataFrameNames接口读取二进制文件输出与预期不符！")

  }

  @Test
  def testGetDataSetMetaData(): Unit = {
    //注入元数据
    provider.getDataSetMetaData("csv", csvModel)
    assertEquals(csvModel.toString, dc.getDataSetMetaData("csv"),"GetDataSetMetaData接口读取csv文件输出与预期不符！")
    provider.getDataSetMetaData("bin", binModel)
    assertEquals(binModel.toString, dc.getDataSetMetaData("bin"),"GetDataSetMetaData接口读取二进制文件输出与预期不符！")
  }


}
