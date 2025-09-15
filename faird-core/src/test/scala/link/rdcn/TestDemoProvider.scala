package link.rdcn

import link.rdcn.ErrorCode._
import link.rdcn.TestBase._
import link.rdcn.server.exception._
import link.rdcn.struct.ValueType.{DoubleType, IntType, LongType}
import link.rdcn.struct._
import link.rdcn.user._
import link.rdcn.util.{ClosableIterator, DataUtils}
import link.rdcn.util.DataUtils.listFiles
import org.json.JSONObject

import java.io.File
import java.nio.file.Paths
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/16 14:19
 * @Modified By:
 */

//用于Demo的Provider
class TestDemoProvider(baseDirString: String = demoBaseDir, subDirString: String = "data") {
  ConfigLoader.init(Paths.get(getResourcePath("tls")).toString)

  val baseDir = getOutputDir(baseDirString, subDirString)
  // 生成的临时目录结构
  val binDir = getOutputDir(baseDirString, Seq(subDirString, "bin").mkString(File.separator))
  val csvDir = getOutputDir(baseDirString, Seq(subDirString, "csv").mkString(File.separator))
  val excelDir = getOutputDir(baseDirString, Seq(subDirString, "excel").mkString(File.separator))
  val jsonDir = getOutputDir(baseDirString, Seq(subDirString, "json").mkString(File.separator))
  val structuredDir = getOutputDir(baseDirString, Seq(subDirString, "structured").mkString(File.separator))
  val dirDir = getOutputDir(baseDirString, Seq(subDirString, "dir", "images").mkString(File.separator))
  val blobDir = getOutputDir(baseDirString, Seq(subDirString, "blob").mkString(File.separator))


  //根据文件生成元信息
  lazy val csvDfInfos = listFiles(csvDir).map(file => {
    DataFrameInfo(Paths.get("/csv").resolve(file.getName).toString.replace("\\", "/"), Paths.get(file.getAbsolutePath).toUri, CSVSource(",", true), StructType.empty.add("id", LongType).add("value", DoubleType))
  })
  lazy val binDfInfos = Seq(
    DataFrameInfo(Paths.get("/").resolve(Paths.get(binDir).getFileName).toString.replace("\\", "/"), Paths.get(binDir).toUri, DirectorySource(false), StructType.binaryStructType))
  lazy val excelDfInfos = listFiles(excelDir).map(file => {
    DataFrameInfo(Paths.get("/excel").resolve(file.getName).toString.replace("\\", "/"), Paths.get(file.getAbsolutePath).toUri, ExcelSource(), StructType.empty.add("id", IntType).add("value", IntType))
  })
  lazy val jsonDfInfos = listFiles(jsonDir).map(file => {
    DataFrameInfo(Paths.get("/json").resolve(file.getName).toString.replace("\\","/"),Paths.get(file.getAbsolutePath).toUri, JSONSource(true), StructType.empty.add("id", LongType).add("value", DoubleType))
  })

  val structuredDataFrame = getStructuredDataFrame(structuredDir)
  lazy val structuredDfInfos = listFiles(structuredDir).map(file => {
    DataFrameInfo(Paths.get("/structured").resolve(file.getName).toString.replace("\\","/"),Paths.get(file.getAbsolutePath).toUri,
      StructuredSource(structuredDataFrame._1,structuredDataFrame._2, structuredDataFrame._3), StructType.empty.add("id", LongType).add("value", DoubleType))
  })
  lazy val dirDfInfos = Seq(
    DataFrameInfo(Paths.get("/").resolve(Paths.get(dirDir).getFileName).toString.replace("\\","/"),Paths.get(dirDir).toUri, DirectorySource(false), StructType.binaryStructType))

  lazy val blobDfInfos = Seq(
    DataFrameInfo(Paths.get("/").resolve(Paths.get(blobDir).getFileName).toString.replace("\\","/"),Paths.get(blobDir).toUri, DirectorySource(false), StructType.binaryStructType))


  val dataSetCsv = DataSet("csv", "1", csvDfInfos.toList)
  val dataSetBin = DataSet("bin", "2", binDfInfos.toList)
  val dataSetExcel = DataSet("excel", "3", excelDfInfos.toList)
  val dataSetJson = DataSet("json", "4", jsonDfInfos.toList)
  val dataSetStructrued = DataSet("structured", "5", structuredDfInfos.toList)
  val dataSetDir= DataSet("dir", "6", dirDfInfos.toList)
  val dataSetBlob = DataSet("blob", "7", blobDfInfos.toList)

  private def getStructuredDataFrame(structuredDir: String): (Seq[Row], StructType, Source) = {
    val file =  listFiles(structuredDir).toSeq.head
    val sampleSize = 1
    val source = Source.fromFile(file)
    val lines: Seq[String] = source.getLines().toSeq
    val head = lines.take(1)
    val headerArray = new ArrayBuffer[String]()
    if (head.nonEmpty) {
      val firstObject = new JSONObject(head.head)
      firstObject.keys().asScala.foreach(key=> headerArray.append(key))
    }
    val sampleObjects = head.map(new JSONObject(_)).iterator.map { jo =>
      // 根据 headerArray 的顺序提取每个 key 对应的值
      headerArray.map(jo.get).map(_.toString).toArray
    }.toArray
    val structType = DataUtils.inferSchema(sampleObjects, headerArray)
    val rows: Seq[Row] = lines.map { line =>
      val jo = new JSONObject(line)
      val seq = headerArray.map(jo.get).map(_.toString)
      Row.fromSeq(seq.toSeq)
    }
    val convertedRows = rows.map(DataUtils.convertStringRowToTypedRow(_, structType))
    (convertedRows, structType, source)
  }



  class TestAuthenticatedUser(userName: String, token: String) extends AuthenticatedUser {
    def getUserName: String = userName
  }

  val authProvider = new AuthProvider {

    override def authenticate(credentials: Credentials): AuthenticatedUser = {
      if (credentials.isInstanceOf[UsernamePassword]) {
        val usernamePassword = credentials.asInstanceOf[UsernamePassword]
        if (usernamePassword.username == null && usernamePassword.password == null) {
          throw new AuthorizationException(USER_NOT_FOUND)
        }
        else if (usernamePassword.username == adminUsername && usernamePassword.password == adminPassword) {
          new TestAuthenticatedUser(adminUsername, genToken())
        } else if (usernamePassword.username == userUsername && usernamePassword.password == userPassword) {
          new TestAuthenticatedUser(adminUsername, genToken())
        }
        else if (usernamePassword.username != "admin") {
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

    override def checkPermission(user: AuthenticatedUser, dataFrameName: String, opList: List[DataOperationType]): Boolean = {
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
    override val dataSetsScalaList: List[DataSet] = List(dataSetCsv, dataSetBin, dataSetExcel, dataSetJson, dataSetStructrued, dataSetDir, dataSetDir)
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      Paths.get(baseDir, relativePath).toString
    }

  }

  // 默认构造函数
  def this() = {
    this(demoBaseDir, "data") // 调用主构造函数
  }
}

