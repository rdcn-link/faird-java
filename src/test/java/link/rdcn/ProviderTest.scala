package link.rdcn

import link.rdcn.client.FairdClient
import link.rdcn.provider.DataProvider
import link.rdcn.server.FlightProducerImpl
import link.rdcn.struct.ValueType.{DoubleType, IntType, LongType, StringType}
import link.rdcn.struct.{CSVSource, DataFrameInfo, DataSet, DirectorySource, StructType}
import link.rdcn.util.DataUtils
import link.rdcn.util.DataUtils.listFiles
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.grapheco.TestDataGenerator
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.io.PrintWriter
import java.nio.file.{Files, Path, Paths}
import scala.util.Random
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.awt.Color

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */

object ProviderTest{
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
    DataUtils.closeAllFileSources()
    TestDataGenerator.cleanupTestData()
  }

  def genModel: Model = {
    ModelFactory.createDefaultModel()
  }

  def getOutputDir(subdir: String): Path = {
    val baseDir = Paths.get(System.getProperty("user.dir")) // 项目根路径
    val outDir = baseDir.resolve("target").resolve(subdir)
    Files.createDirectories(outDir)
    outDir
  }

  def generalData(): Unit = {
    val csvDir = getOutputDir("test_output/csv")
    val pngDir = getOutputDir("test_output/png")

    // 创建文件夹
    Files.createDirectories(csvDir)
    Files.createDirectories(pngDir)

    // 生成 5 个 CSV 文件
    for (i <- 1 to 5) {
      val file = csvDir.resolve(s"file_$i.csv").toFile
      val writer = new PrintWriter(file)
      writer.println("id,name,value")
      for (j <- 1 to 5) {
        val id = j
        val name = s"name${Random.nextInt(100)}"
        val value = Random.nextDouble()
        writer.println(s"$id,$name,$value")
      }
      writer.close()
    }

    // 生成 5 个 PNG 文件（100x100随机像素）
    for (i <- 1 to 5) {
      val image = new BufferedImage(100, 100, BufferedImage.TYPE_INT_RGB)
      for (x <- 0 until 100; y <- 0 until 100) {
        val color = new Color(Random.nextInt(256), Random.nextInt(256), Random.nextInt(256))
        image.setRGB(x, y, color.getRGB)
      }
      val outputFile = pngDir.resolve(s"image_$i.png").toFile
      ImageIO.write(image, "png", outputFile)
    }

    println("文件生成完成。")
  }
}

class ProviderTest {
  val provider = ProviderTest.dataProvider
  val csvModel: Model = ProviderTest.genModel
  val binModel: Model = ProviderTest.genModel

  @Test
  def testAPI(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101")
//    println(provider.getDataSetMetaData("csv", genModelRef.apply()))


    assertEquals(provider.listDataSetNames().toSet,dc.listDataSetNames().toSet)
    assertEquals(provider.listDataFrameNames("csv").toSet, dc.listDataFrameNames("csv").toSet)
    assertEquals(provider.listDataFrameNames("bin").toSet, dc.listDataFrameNames("bin").toSet)
    //注入元数据
    provider.getDataSetMetaData("csv", csvModel)
    assertEquals(csvModel.toString, dc.getDataSetMetaData("csv"))
    provider.getDataSetMetaData("bin", binModel)
    assertEquals(binModel.toString, dc.getDataSetMetaData("bin"))


//    val df = dc.open("C:\\Users\\Yomi\\PycharmProjects\\Faird\\Faird\\target\\test_output\\csv\\data_1.csv")
//    df.limit(10).foreach(
//      println
//    )
  }

  @Test
  def m1(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101")

    dc.listDataSetNames().foreach(println)
    dc.listDataFrameNames("csv").foreach(println)
    val meta = dc.getDataSetMetaData("csv")
    println(meta)

    val df = dc.open("C:\\Users\\Yomi\\PycharmProjects\\Faird\\Faird\\target\\test_output\\csv\\data_1.csv")
    df.limit(10).foreach(
      println
    )
  }

  @Test
  def m2(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101")

    dc.listDataFrameNames("bin").foreach(println)
    val meta = dc.getDataSetMetaData("bin")
    println(meta)

//    val df = dc.open("C:\\Users\\Yomi\\PycharmProjects\\Faird\\Faird\\target\\test_output\\bin\\data_1.csv")
    val df = dc.open("C:\\Users\\Yomi\\PycharmProjects\\Faird\\Faird\\target\\test_output\\bin")

    val cnt=0
    df.foreach(
      println
    )
  }
}
