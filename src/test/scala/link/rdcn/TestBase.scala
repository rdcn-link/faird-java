package link.rdcn

import link.rdcn.ErrorCode._
import link.rdcn.client.FairdClient
import link.rdcn.server.FairdServer
import link.rdcn.server.exception._
import link.rdcn.struct.ValueType.{DoubleType, LongType}
import link.rdcn.struct.{CSVSource, DataFrameInfo, DataSet, DirectorySource, StructType}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, DataOperationType, UsernamePassword}
import link.rdcn.util.{DataProviderImpl, DataUtils}
import link.rdcn.util.DataUtils.listFiles
import org.apache.commons.io.FileUtils
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import java.io.{BufferedWriter, File, FileOutputStream, FileWriter}
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

trait TestBase {


}

object TestBase {


  val baseDir = getOutputDir("test_output")
  // 生成的临时目录结构
  val binDir = getOutputDir("test_output/bin")
  val csvDir = getOutputDir("test_output/csv")

  // 文件数量配置
  private val binFileCount = 3
  private val csvFileCount = 3

  //必须在DfInfos前执行一次！！！
  generateTestData()

  //根据文件生成元信息
  lazy val csvDfInfos = listFiles(csvDir.toString).map(file => {
    DataFrameInfo(file.getAbsolutePath, CSVSource(",", true), StructType.empty.add("id", LongType).add("value", DoubleType))
  })
  lazy val binDfInfos = Seq(
    DataFrameInfo(binDir.toString, DirectorySource(false), StructType.binaryStructType))

  val dataSetCsv = DataSet("csv", "1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin", "2", binDfInfos.toList)

  val adminUsername = "admin"
  val adminPassword = "admin"
  val userUsername = "user"
  val userPassword = "user"
  val anonymousUsername = "anonymous"

  //权限
  val permissions = Map(
    "admin" -> Set(s"$csvDir\\data_1.csv", s"$csvDir\\invalid.csv", s"$binDir", s"/csv/data_1.csv", "/bin",
      "/csv/data_2.csv","/bin/data_1.csv", "/csv/invalid.csv")
  )

  //生成Token
  val genToken = () => UUID.randomUUID().toString


  class TestAuthenticatedUser(userName: String, token: String) extends AuthenticatedUser {
    def getUserName: String = userName

  }


  val authprovider = new AuthProvider {

    override def authenticate(credentials: Credentials): AuthenticatedUser = {
      if (credentials.isInstanceOf[UsernamePassword]) {
        val usernamePassword = credentials.asInstanceOf[UsernamePassword]
        if (usernamePassword.userName == null && usernamePassword.password == null) {
          throw new AuthorizationException(USER_NOT_FOUND)
        }
        else if (usernamePassword.userName == adminUsername && usernamePassword.password == adminPassword) {
          new TestAuthenticatedUser(adminUsername, genToken())
        } else if (usernamePassword.userName == userUsername && usernamePassword.password == userPassword) {
          new TestAuthenticatedUser(adminUsername, genToken())
        }
        else if (usernamePassword.userName != "admin") {
          throw new AuthorizationException(USER_NOT_FOUND)
        } else {
          throw new AuthorizationException(INVALID_CREDENTIALS)
        }
      } else if (credentials == Credentials.ANONYMOUS) {
        new TestAuthenticatedUser(anonymousUsername, genToken())
      }
      else {
        throw new AuthorizationException(INVALID_CREDENTIALS)
      }
    }

    override def checkPermission(user: AuthenticatedUser, dataFrameName: String, opList: java.util.List[DataOperationType]): Boolean = {
      val userName = user.asInstanceOf[TestAuthenticatedUser].getUserName
      if (userName == anonymousUsername)
        throw new AuthorizationException(USER_NOT_LOGGED_IN)
      permissions.get(userName) match { // 用 get 避免 NoSuchElementException
        case Some(allowedFiles) => allowedFiles.contains(dataFrameName)
        case None => false // 用户不存在或没有权限
      }
    }
  }
  val dataProvider: DataProviderImpl = new DataProviderImpl() {
    override val dataSetsScalaList: List[DataSet] = List(dataSetCsv, dataSetBin)
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      getOutputDir("test_output/bin").resolve(relativePath).toString
    }
    //    override def getDataFrameSize(dataFrameName: String): lang.Long = 0L

  }

  //  val producer = new FlightProducerImpl(allocator, location, dataProvider, authprovider)
  private var fairdServer: Option[FairdServer] = None
  var dc: FairdClient = _
  val configCache = ConfigLoader.fairdConfig
  var expectedHostInfo: String = _

  @BeforeAll
  def startServer(): Unit = {
    generateTestData()
    getServer
    connectClient

  }

  @AfterAll
  def stop(): Unit = {
    stopServer()
    DataUtils.closeAllFileSources()
    cleanupTestData()
  }

  def getServer: FairdServer = synchronized {
    if (fairdServer.isEmpty) {
      val s = new FairdServer(dataProvider, authprovider, getResourcePath(""))

      s.start()
      //      println(s"Server (Location): Listening on port ${s.getPort}")
      fairdServer = Some(s)
      expectedHostInfo =
        s"""
           |faird.hostName: ${ConfigLoader.fairdConfig.hostName}
           |faird.hostTitle: ${ConfigLoader.fairdConfig.hostTitle}
           |faird.hostPosition: ${ConfigLoader.fairdConfig.hostPosition}
           |faird.hostDomain: ${ConfigLoader.fairdConfig.hostDomain}
           |faird.hostPort: ${ConfigLoader.fairdConfig.hostPort}
           |""".stripMargin
    }
    fairdServer.get
  }

  def connectClient: Unit = synchronized {
    dc = FairdClient.connect("dacp://0.0.0.0:3101", UsernamePassword(adminUsername, adminPassword))
  }

  def stopServer(): Unit = synchronized {
    fairdServer.foreach(_.close())
    fairdServer = None
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

  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName)))
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    url.getPath
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

  def getExpectedDataFrameSize(dataFrameName: String): Long = {
    dataProvider.dataSetsScalaList.foreach(ds => {
      val dfInfo = ds.getDataFrameInfo(dataFrameName)
      if (dfInfo.nonEmpty) return DataUtils.countLinesFast(new File(dfInfo.get.name))
    })
    -1L
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

