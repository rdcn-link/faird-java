/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/16 14:19
 * @Modified By:
 */
package link.rdcn

import link.rdcn.ErrorCode._
import link.rdcn.server.exception._
import link.rdcn.struct.ValueType.{DoubleType, LongType}
import link.rdcn.struct._
import link.rdcn.user._
import link.rdcn.util.DataUtils._
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.UUID

//用于Demo的Provider
class TestProvider(baseDirString: String = "src/test/demo", subDirString: String = "data") {


  ConfigLoader.init(getResourcePath("/conf/faird.conf"))

  val baseDir = getOutputDir(baseDirString, subDirString)
  // 生成的临时目录结构
  val binDir = getOutputDir(baseDirString, Seq(subDirString, "bin").mkString(File.separator))
  val csvDir = getOutputDir(baseDirString, Seq(subDirString, "csv").mkString(File.separator))

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
    adminUsername -> Set("/csv/data_1.csv", "/bin",
      "/csv/data_2.csv", "/csv/data_1.csv", "/csv/invalid.csv")
  )

  //生成Token
  val genToken = () => UUID.randomUUID().toString


  class TestAuthenticatedUser(userName: String, token: String) extends AuthenticatedUser {
    def getUserName: String = userName

  }


  val authProvider = new AuthProvider {

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
      getOutputDir("", "").resolve(relativePath).toString
    }

  }


  // 默认构造函数
  def this() = {
    this("src/test/demo", "data") // 调用主构造函数
  }

  def genModel: Model = {
    ModelFactory.createDefaultModel()
  }

  def getOutputDir(dir: String, subDirString: String): Path = {
    val baseDir = Paths.get(System.getProperty("user.dir")) // 项目根路径
    val outDir = baseDir.resolve(dir).resolve(subDirString)
    Files.createDirectories(outDir)
    outDir
  }

  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName)))
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    url.getPath
  }

}

