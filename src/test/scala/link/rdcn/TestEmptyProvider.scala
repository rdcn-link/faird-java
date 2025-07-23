/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/16 13:34
 * @Modified By:
 */
package link.rdcn
import link.rdcn.provider.DataStreamSource
import link.rdcn.server.FlightProducerImpl
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, DataOperationType}
import org.apache.arrow.flight.Location
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.nio.file.{Files, Path, Paths}
import java.util.UUID

trait TestEmptyProvider{

}

/***
 * 用于不需要生成数据的测试的Provider
 */
object TestEmptyProvider {
  ConfigLoader.init(getResourcePath(""))

  val outputDir = getOutputDir("test_output","output")

  val adminUsername = "admin"
  val adminPassword = "admin"
  val userUsername = "user"
  val userPassword = "user"
  val anonymousUsername = "anonymous"


  val location = Location.forGrpcInsecure(ConfigLoader.fairdConfig.hostPosition, ConfigLoader.fairdConfig.hostPort)
  val allocator: BufferAllocator = new RootAllocator()
  val emptyAuthProvider = new AuthProvider {
    override def authenticate(credentials: Credentials): AuthenticatedUser = {
      null
    }

    /**
     * 判断用户是否具有某项权限
     */
    override def checkPermission(user: AuthenticatedUser, dataFrameName: String, opList: java.util.List[DataOperationType]): Boolean = ???
  }

  val emptyDataProvider: DataProviderImpl = new DataProviderImpl() {
    override val dataSetsScalaList: List[DataSet] = List.empty
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      null
    }

    override def getDataStreamSource(dataFrameName: String): DataStreamSource = ???
  }

  val producer = new FlightProducerImpl(allocator, location, emptyDataProvider, emptyAuthProvider)
  val configCache = ConfigLoader.fairdConfig


  def getOutputDir(subDirs: String*): String = {
    val outDir = Paths.get(System.getProperty("user.dir"), subDirs: _*) // 项目根路径
    Files.createDirectories(outDir)
    outDir.toString
  }

  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName)))
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    val nativePath: Path = Paths.get(url.toURI())
    nativePath.toString
  }

  //生成Token
  val genToken = () => UUID.randomUUID().toString


  class TestAuthenticatedUser(userName: String, token: String) extends AuthenticatedUser {
    def getUserName: String = userName
  }

  def genModel: Model = {
    ModelFactory.createDefaultModel()
  }


}
