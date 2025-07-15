package link.rdcn

import link.rdcn.ErrorCode._
import link.rdcn.client.FairdClient
import link.rdcn.server.FairdServer
import link.rdcn.server.exception._
import link.rdcn.struct.ValueType.{DoubleType, LongType}
import link.rdcn.struct._
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, DataOperationType, UsernamePassword}
import link.rdcn.util.{DataProviderImpl, DataUtils, SharedValue}
import link.rdcn.util.DataUtils.listFiles
import link.rdcn.util.SharedValue.getOutputDir
import org.apache.commons.io.FileUtils
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import java.io.{BufferedWriter, File, FileOutputStream, FileWriter}
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

trait Server {


}

object Server {


  val baseDir = getOutputDir("data")
  // 生成的临时目录结构
  val binDir = getOutputDir("data/bin")
  val csvDir = getOutputDir("data/csv")

  //根据文件生成元信息
  lazy val csvDfInfos = listFiles(csvDir.toString).map(file => {
    DataFrameInfo(file.getAbsolutePath, CSVSource(",", true), StructType.empty.add("id", LongType).add("value", DoubleType))
  })
  lazy val binDfInfos = Seq(
    DataFrameInfo(binDir.toString, DirectorySource(false), StructType.binaryStructType))

  val dataSetCsv = DataSet("csv", "1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin", "2", binDfInfos.toList)

  val adminUsername = "admin@instdb.cn"
  val adminPassword = "admin001"
  val userUsername = "user"
  val userPassword = "user"
  val anonymousUsername = "anonymous"

  //权限
  val permissions = Map(
    adminUsername -> Set(s"$csvDir\\data_1.csv", s"$csvDir\\invalid.csv", s"$binDir", s"/csv/data_1.csv", "/bin",
      "/csv/data_2.csv", "/bin/data_1.csv", "/csv/invalid.csv")
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
      getOutputDir("data/bin").resolve(relativePath).toString
    }

  }

  private var fairdServer: Option[FairdServer] = None
  val configCache = ConfigLoader.fairdConfig
  var expectedHostInfo: String = _


  def runServer: FairdServer = synchronized {
    if (fairdServer.isEmpty) {
      val s = new FairdServer(dataProvider, authprovider, getResourcePath(""))

      s.start()
      //      println(s"Server (Location): Listening on port ${s.getPort}")
      fairdServer = Some(s)
    }
      fairdServer.get
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
      val outDir = baseDir.resolve("demo").resolve(subdir)
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
    def getExpectedDataFrameSize(dataFrameName: String): Long = {
      dataProvider.dataSetsScalaList.foreach(ds => {
        val dfInfo = ds.getDataFrameInfo(dataFrameName)
        if (dfInfo.nonEmpty) return DataUtils.countLinesFast(new File(dfInfo.get.name))
      })
      -1L
    }

}

