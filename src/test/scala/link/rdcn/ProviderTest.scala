package link.rdcn

import link.rdcn.client.FairdClient
import link.rdcn.provider.DataProviderImplByDataSetList
import link.rdcn.server.FlightProducerImpl
import link.rdcn.struct.ValueType.{DoubleType, IntType, StringType}
import link.rdcn.struct.{CSVSource, DataFrameInfo, DataSet, StructType}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials}
import link.rdcn.util.DataUtils.listFiles
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test}

import java.io.PrintWriter
import java.nio.file.{Files, Path, Paths}
import scala.util.Random
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.awt.Color
import java.util
import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */

object ProviderTest{
  val location: Location = Location.forGrpcInsecure(ConfigLoader.fairdConfig.getHostPosition, ConfigLoader.fairdConfig.getHostPort)
  val allocator: BufferAllocator = new RootAllocator()
  generalData
  val dfInfos = listFiles(getOutputDir("test_output/csv").toString).map(file => {
    DataFrameInfo(file.getAbsolutePath, CSVSource(",", true), StructType.empty.add("id", IntType).add("name", StringType).add("value", DoubleType))
  })
  val dataSetCsv = DataSet("csv","1", dfInfos.toList)

  val producer = new FlightProducerImpl(allocator, location, new DataProviderImplByDataSetList(){
    override val dataSetsScalaList: List[DataSet] = List(dataSetCsv)
  }, new AuthProvider {
      /**
       * 用户认证，成功返回认证后的用户信息，失败抛出 AuthException 异常
       */
      override def authenticate(credentials: Credentials): AuthenticatedUser = {
        new AuthenticatedUser("", new java.util.HashSet[String](),new java.util.HashSet[String]())
      }

      /**
       * 判断用户是否具有某项权限
       */
      override def authorize(user: AuthenticatedUser, dataFrameName: String): Boolean = true
  })



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

  @Test
  def m1(): Unit = {
    val dc = FairdClient.connect("dacp://0.0.0.0:3101")
    dc.listDataSetNames().foreach(println)
    dc.listDataFrameNames("csv").foreach(println)
    val meta = dc.getDataSetMetaData("csv")
    println(meta)

    val df = dc.open("/Users/renhao/IdeaProjects/faird-java/target/test_output/csv/file_1.csv")
    df.foreach(println)
  }
}
