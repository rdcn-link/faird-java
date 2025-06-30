package link.rdcn

import io.grpc.StatusRuntimeException
import link.rdcn.client.FairdClient
import link.rdcn.provider.DataProviderImplByDataSetList
import link.rdcn.server.FlightProducerImpl
import link.rdcn.struct.ValueType.{DoubleType, IntType}
import link.rdcn.struct.{CSVSource, DataFrameInfo, DataSet, DirectorySource, StructType}
import link.rdcn.user.exception.{InvalidCredentialsException, TokenExpiredException, UserNotFoundException}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, UsernamePassword}
import link.rdcn.util.DataUtils
import link.rdcn.util.DataUtils.listFiles
import org.apache.arrow.flight.{FlightServer, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.commons.io.FileUtils
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import java.io.FileOutputStream
import java.nio.file.{Files, Path, Paths}
import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConverters.setAsJavaSetConverter

trait TestBase

object TestBase {

  val location = Location.forGrpcInsecure(ConfigLoader.fairdConfig.getHostPosition, ConfigLoader.fairdConfig.getHostPort)
  val allocator: BufferAllocator = new RootAllocator()

  val baseDir = getOutputDir("test_output")
  // 生成的临时目录结构
  val binDir = getOutputDir("test_output/bin")
  val csvDir = getOutputDir("test_output/csv")

  // 文件数量配置
  private val binFileCount = 3
  private val csvFileCount = 3

  // 生成测试数据
  generateTestData()

  //根据文件生成元信息
  val csvDfInfos = listFiles(csvDir.toString).map(file => {
    DataFrameInfo(file.getAbsolutePath, CSVSource(",", true), StructType.empty.add("id", IntType).add("value", DoubleType))
  })
  val binDfInfos = Seq(
    DataFrameInfo(binDir.toString, DirectorySource(false), StructType.binaryStructType))

  val dataSetCsv = DataSet("csv", "1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin", "2", binDfInfos.toList)

  val adminUsername = "admin"
  val adminPassword = "admin"
  val userUsername = "user"
  val userPassword = "user"






  val authprovider = new AuthProvider {

    override def authenticate(credentials: Credentials): AuthenticatedUser = {
      if (credentials.isInstanceOf[UsernamePassword]) {
        val usernamePassword = credentials.asInstanceOf[UsernamePassword]
        if (usernamePassword.getUsername == null && usernamePassword.getPassword == null) {
//          throw new StatusRuntimeException(io.grpc.Status.UNAUTHENTICATED.withDescription("User not logged in"))
            throw new UserNotFoundException()
        }
        else if (usernamePassword.getUsername == adminUsername && usernamePassword.getPassword == adminPassword) {
          new AuthenticatedUser(adminUsername, Set("admin").asJava, Set.empty[String].asJava)
        } else if (usernamePassword.getUsername == userUsername && usernamePassword.getPassword == userPassword) {
          new AuthenticatedUser(userUsername, Set("user").asJava, Set.empty[String].asJava)
        }
        else if (usernamePassword.getUsername != "admin") {
          throw new UserNotFoundException()
        } else {
          throw new InvalidCredentialsException()
        }
      } else {
        throw new TokenExpiredException()
      }
    }

    override def authorize(user: AuthenticatedUser, dataFrameName: String): Boolean = {
      if (user.getUserId == "admin") true
      else user.getPermissions.contains(s"$dataFrameName")
    }

  }
  val dataProvider: DataProviderImplByDataSetList = new DataProviderImplByDataSetList() {
    override val dataSetsScalaList: List[DataSet] = List(dataSetCsv, dataSetBin)
  }

  val producer = new FlightProducerImpl(allocator, location, dataProvider, authprovider)
  val flightServer: FlightServer = FlightServer.builder(allocator, location, producer).build()
  lazy val dc: FairdClient = FairdClient.connect("dacp://0.0.0.0:3101", adminUsername, adminPassword)

  @BeforeAll
  def startServer(): Unit = {
    flightServer.start()
    println(s"Server (Location): Listening on port ${flightServer.getPort}")
  }

  @AfterAll
  def stopServer(): Unit = {
    producer.close()
    flightServer.shutdown()

    DataUtils.closeAllFileSources()
    cleanupTestData()
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

  // 生成所有测试数据
  def generateTestData(): Unit = {
    println("Starting test data generation...")
    val startTime = System.currentTimeMillis()

    createDirectories()
    generateBinaryFiles()
    generateCsvFiles()

    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Test data generation completed in ${duration}s")
    printDirectoryInfo()
  }

  // 清理所有测试数据
  def cleanupTestData(): Unit = {
    println("Cleaning up test data...")
    val startTime = System.currentTimeMillis()

    if (Files.exists(getOutputDir("test_output"))) {
      FileUtils.deleteDirectory(baseDir.toFile)
      println(s"Deleted directory: ${baseDir.toAbsolutePath}")
    }

    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Cleanup completed in ${duration}s")
  }

  private def createDirectories(): Unit = {
    Files.createDirectories(binDir)
    Files.createDirectories(csvDir)
    println(s"Created directory structure at ${baseDir.toAbsolutePath}")
  }

  private def generateBinaryFiles(): Unit = {
    println(s"Generating $binFileCount binary files (~1GB each)...")
    (1 to binFileCount).foreach { i =>
      val fileName = s"binary_data_$i.bin"
      val filePath = binDir.resolve(fileName)
      val startTime = System.currentTimeMillis()
      val size = 1024 * 1024 * 1024 // 1GB
      var fos: FileOutputStream = null
      try {
        fos = new FileOutputStream(filePath.toFile)
        val buffer = new Array[Byte](1024 * 1024) // 1MB buffer
        var bytesWritten = 0L
        while (bytesWritten < size) {
          fos.write(buffer)
          bytesWritten += buffer.length
        }
      } finally {
        if (fos != null) fos.close()
      }
      val duration = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"   Generated ${filePath.getFileName} (${formatSize(size)}) in ${duration}s")
    }
  }


  private def generateCsvFiles(): Unit = {
    println(s"Generating $csvFileCount CSV files with 100 million rows each...")
    (1 to csvFileCount).foreach { i =>
      val fileName = s"data_$i.csv"
      val filePath = csvDir.resolve(fileName).toFile
      val startTime = System.currentTimeMillis()
      val rows = 10000 // 1 亿行
      var writer: BufferedWriter = null // 声明为 var，方便 finally 块中访问

      try {
        writer = new BufferedWriter(new FileWriter(filePath), 1024 * 1024) // 1MB 缓冲区
        writer.write("id,value\n") // 写入表头

        for (row <- 1 to rows) {
          writer.append(row.toString).append(',').append(math.random.toString).append('\n')
          if (row % 1000000 == 0) writer.flush() // 每百万行刷一次
        }

        val duration = (System.currentTimeMillis() - startTime) / 1000.0
        println(f"   Generated ${filePath.getName} with $rows rows in $duration%.2fs")

      } catch {
        case e: Exception =>
          println(s"Error generating file ${filePath.getName}: ${e.getMessage}")
          throw e

      } finally {
        if (writer != null) {
          try writer.close()
          catch {
            case e: Exception => println(s"Error closing writer: ${e.getMessage}")
          }
        }
      }
    }
  }


  private def formatSize(bytes: Long): String = {
    if (bytes < 1024) s"${bytes}B"
    else if (bytes < 1024 * 1024) s"${bytes / 1024}KB"
    else if (bytes < 1024 * 1024 * 1024) s"${bytes / (1024 * 1024)}MB"
    else s"${bytes / (1024 * 1024 * 1024)}GB"
  }

  private def printDirectoryInfo(): Unit = {
    println("\n Generated Data Summary:")
    printDirectorySize(binDir, "Binary Files")
    printDirectorySize(csvDir, "CSV Files")
    println("----------------------------------------\n")
  }

  private def printDirectorySize(dir: Path, label: String): Unit = {
    if (Files.exists(dir)) {
      val size = Files.walk(dir)
        .filter(p => Files.isRegularFile(p))
        .mapToLong(p => Files.size(p))
        .sum()
      println(s"   $label: ${formatSize(size)} (${Files.list(dir).count()} files)")
    }
  }

}

