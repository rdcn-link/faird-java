package link.rdcn.client.dacp

import link.rdcn.client.UrlValidator
import link.rdcn.client.dftp.DftpClient
import link.rdcn.struct.DataFrame
import link.rdcn.user.Credentials
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.io.StringReader

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/18 16:30
 * @Modified By:
 */
class FairdClient private(url: String, port: Int, useTLS: Boolean = false) extends DftpClient(url, port, useTLS) {


  def listDataSetNames(): Seq[String] =
    get("/listDataSetNames").mapIterator[Seq[String]](iter =>{
      iter.map(row => row.getAs[String](0)).toSeq
    })

  def listDataFrameNames(dsName: String): Seq[String] =
    get(s"/listDataFrameNames/$dsName").mapIterator[Seq[String]](iter =>{
      iter.map(row => row.getAs[String](0)).toSeq
    })

  def getDataSetMetaData(dsName: String): Model = {
    val rdfString = get(s"/getDataSetMetaData/$dsName").collect().head.getAs[String](0)
    val model = ModelFactory.createDefaultModel()
    val reader = new StringReader(rdfString)
    model.read(reader, null, "RDF/XML")
    model
  }

  def getDataFrameSize(dataFrameName: String): Long =
    get(s"/getDataFrameSize/$dataFrameName").collect().head.getAs[Long](0)

  def getHostInfo: Map[String, String] = {
    val df = get(s"/getHostInfo")
    val schema = df.schema.columns
    schema.zip(df.collect().head.toSeq).map(kv => (kv._1.name, kv._2.toString)).toMap
  }

  def getServerResourceInfo: Map[String, String] = {
    val df = get(s"/getServerResourceInfo")
    val schema = df.schema.columns
    schema.zip(df.collect().head.toSeq).map(kv => (kv._1.name, kv._2.toString)).toMap
  }

  override def get(dataFrameName: String): DataFrame = {
    super.get(dacpUrlPrefix + dataFrameName)
  }

  private val dacpUrlPrefix: String = s"dacp://$url:$port"
}

object FairdClient {
  val protocolSchema = "dacp"
  private val urlValidator = UrlValidator(protocolSchema)
  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): FairdClient = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new FairdClient(parsed._1, parsed._2.getOrElse(3101))
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
  def connectTLS(url: String, credentials: Credentials = Credentials.ANONYMOUS): FairdClient = {
    urlValidator.validate(url) match {
      case Right(parsed) =>
        val client = new FairdClient(parsed._1, parsed._2.getOrElse(3101), true)
        client.login(credentials)
        client
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
