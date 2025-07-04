package link.rdcn.util

import link.rdcn.ConfigLoader
import link.rdcn.server.FlightProducerImpl
import link.rdcn.struct.DataSet
import link.rdcn.user.{AuthProvider, AuthenticatedUser, Credentials, UsernamePassword}
import org.apache.arrow.flight.Location
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import java.nio.file.{Files, Path, Paths}
import java.util.UUID

object SharedValue {
  ConfigLoader.init(getResourcePath("faird.conf"))
  ConfigLoader.init(getResourcePath("faird.conf"))

  val adminUsername = "admin"
  val adminPassword = "admin"
  val userUsername = "user"
  val userPassword = "user"
  val anonymousUsername = "anonymous"


  val location = Location.forGrpcInsecure(ConfigLoader.fairdConfig.getHostPosition, ConfigLoader.fairdConfig.getHostPort)
  val allocator: BufferAllocator = new RootAllocator()
  val emptyAuthProvider = new AuthProvider {
    override def authenticate(credentials: Credentials): AuthenticatedUser = {
      null
    }

    override def authorize(user: AuthenticatedUser, dataFrameName: String): Boolean = {
      true
    }
  }

  val emptyDataProvider: DataProviderImpl = new DataProviderImpl() {
    override val dataSetsScalaList: List[DataSet] = List.empty
    override val dataFramePaths: (String => String) = (relativePath: String) => {
      null
    }
  }

  val producer = new FlightProducerImpl(allocator, location, emptyDataProvider, emptyAuthProvider)
  val configCache = ConfigLoader.fairdConfig


  def getOutputDir(subdir: String): Path = {
    val baseDir = Paths.get(System.getProperty("user.dir")) // 项目根路径
    val outDir = baseDir.resolve("target").resolve(subdir)
    Files.createDirectories(outDir)
    outDir
  }

  def getHomeDir: Path = {
    val baseDir = Paths.get(System.getProperty("user.dir")) // 项目根路径
    val outDir = baseDir.resolve("src").resolve("main").resolve("resources")
    Files.createDirectories(outDir)
    outDir
  }

  def getResourcePath(resourceName: String): String = {
    val url = Option(getClass.getClassLoader.getResource(resourceName))
      .orElse(Option(getClass.getResource(resourceName)))
      .getOrElse(throw new RuntimeException(s"Resource not found: $resourceName"))
    url.getPath
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
