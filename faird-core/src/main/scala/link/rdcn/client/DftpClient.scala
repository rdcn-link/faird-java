package link.rdcn.client

import link.rdcn.SimpleSerializer
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics}
import link.rdcn.struct.DataFrame
import link.rdcn.user.{AnonymousCredentials, Credentials, UsernamePassword}
import link.rdcn.util.ClientUtils
import link.rdcn.util.ClientUtils.{getArrayBytesResult, getBytesFromVectorSchemaRoot, getListStringByResult, getMapByJsonString, getSingleLongByResult, getSingleStringByResult}
import org.apache.arrow.flight.{Action, FlightClient, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.{VarCharVector, VectorSchemaRoot}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang

import java.io.{FileInputStream, InputStreamReader, StringReader}
import java.nio.file.Paths
import java.util.Properties
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter, seqAsJavaListConverter}

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 19:47
 * @Modified By:
 */
abstract class DftpClient(url: String, port: Int, useTLS: Boolean = false) {

  val location = {
    if (useTLS) {
      val props = new Properties()
      val confPathURI = this.getClass.getProtectionDomain().getCodeSource().getLocation().toURI
      val fis = new InputStreamReader(new FileInputStream(Paths.get(confPathURI).resolve("user.conf").toString), "UTF-8")
      try props.load(fis) finally fis.close()
      System.setProperty("javax.net.ssl.trustStore", Paths.get(props.getProperty("tls.path")).toString())
      Location.forGrpcTls(url, port)
    } else
      Location.forGrpcInsecure(url, port)
  }
  val allocator: BufferAllocator = new RootAllocator()
  ClientUtils.init(allocator)
  val flightClient = FlightClient.builder(allocator, location).build()
  private var userToken: Option[String] = None

  def login(credentials: Credentials): Unit = {
    val clientAuthHandler = new FlightClientAuthHandler(credentials)
    flightClient.authenticate(clientAuthHandler)
    userToken = Some(clientAuthHandler.getSessionToken)
    val paramFields: Seq[Field] = List(
      new Field("credentials", FieldType.nullable(new ArrowType.Utf8), null),
    )
    val schema = new Schema(paramFields.asJava)
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val credentialsVector = vectorSchemaRoot.getVector("credentials").asInstanceOf[VarCharVector]
    credentialsVector.allocateNew(1)
    val userPassword = credentials match {
      case _: UsernamePassword => credentials.asInstanceOf[UsernamePassword]
      case AnonymousCredentials => UsernamePassword("anonymous", "anonymous")
      case _ => throw new IllegalArgumentException("Unsupported credential type")
    }
    val credentialsJsonString =
      s"""
         |{
         |    "username" : "${userPassword.username}",
         |    "password" : "${userPassword.password}"
         |}
         |""".stripMargin.stripMargin.replaceAll("\n", "").replaceAll("\\s+", " ")
    credentialsVector.set(0, credentialsJsonString.getBytes("UTF-8"))
    vectorSchemaRoot.setRowCount(1)
    val body = getBytesFromVectorSchemaRoot(vectorSchemaRoot)
    val result = flightClient.doAction(new Action(s"login.${userToken.orNull}", body)).asScala
    result.hasNext
  }

  def get(url: String, body: Array[Byte] = Array.emptyByteArray): DataFrame

  def put(dataFrame: DataFrame): Unit
}

class DacpClient(url: String, port: Int, useTLS: Boolean = false) extends DftpClient(url, port, useTLS) {

  override def get(url: String, body: Array[Byte]): DataFrame = {
    val path = DacpUrlValidator.extractPath(url) match {
      case Right(path) => path
      case Left(message) => throw new IllegalArgumentException(message)
    }
    path match {
      case "/listDataSetNames" =>
        val dataSetNames = flightClient.doAction(new Action(path)).asScala
        DataFrame.fromSeq(getListStringByResult(dataSetNames))
      case actionType if actionType.startsWith("/listDataFrameNames") => {
        val dataFrameNames = flightClient.doAction(new Action(path)).asScala
        DataFrame.fromSeq(getListStringByResult(dataFrameNames))
      }
      case actionType if actionType.startsWith("/getSchema") =>
        val schema = flightClient.doAction(new Action(path)).asScala
        DataFrame.fromSeq(Seq(getSingleStringByResult(schema)))

      case actionType if actionType.startsWith("/getDataSetMetaData") => {
        val dataSetMetaData = flightClient.doAction(new Action(path)).asScala
        val modelString = getSingleStringByResult(dataSetMetaData)
        val model = ModelFactory.createDefaultModel
        val reader = new StringReader(modelString)
        model.read(reader, null, Lang.TTL.getName)
        DataFrame.fromMap(ClientUtils.modelToMap(model).values.toSeq)
      }
      case actionType if actionType.startsWith("/getDataFrameSize") => {
        val dataFrameSize = flightClient.doAction(new Action(path)).asScala
        DataFrame.fromSeq(Seq(getSingleLongByResult(dataFrameSize)))
      }

      case actionType if actionType.startsWith("/getHostInfo") =>
        val hostInfo = flightClient.doAction(new Action(path)).asScala
        DataFrame.fromMap(Seq(getMapByJsonString(getSingleStringByResult(hostInfo))))

      case actionType if actionType.startsWith("/getServerResourceInfo") =>
        val serverResourceInfo = flightClient.doAction(new Action(path)).asScala
        DataFrame.fromMap(Seq(getMapByJsonString(getSingleStringByResult(serverResourceInfo))))
      case actionType if actionType.startsWith("/getSchemaURI") =>
        val dfName = actionType.stripPrefix("/getSchemaURI/")
        val schemaURI = flightClient.doAction(new Action(s"getSchemaURI.$dfName")).asScala
        DataFrame.fromSeq(Seq(getSingleStringByResult(schemaURI)))

      case actionType if actionType.startsWith("/getStatistics") => {
        val dataFrameStatistics = flightClient.doAction(new Action(actionType)).asScala
        val statistics = SimpleSerializer.deserialize(getArrayBytesResult(dataFrameStatistics)).asInstanceOf[DataFrameStatistics]
        DataFrame.fromMap(Seq(Map("rowCount" -> statistics.rowCount, "byteSize" -> statistics.byteSize)))
      }
    }
  }

  override def put(dataFrame: DataFrame): Unit = ???
}

object DacpClient {
  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClient = {
    DacpUrlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101))
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }


  def connectTLS(url: String, credentials: Credentials = Credentials.ANONYMOUS): DacpClient = {
    DacpUrlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new DacpClient(parsed._1, parsed._2.getOrElse(3101), true)
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}