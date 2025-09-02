package link.rdcn.client.dacp

import link.rdcn.client.UrlValidator
import link.rdcn.client.dftp.DftpClient
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics}
import link.rdcn.struct.{DFRef, DataFrame}
import link.rdcn.user.Credentials
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.{JSONArray, JSONObject}

import java.io.StringReader
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/18 16:30
 * @Modified By:
 */
class FairdClient(host: String, port: Int, useTLS: Boolean = false) extends DftpClient(host, port, useTLS) {

  override val prefixSchema: String = "dacp"
  def listDataSetNames(): Seq[String] = getDataSetInfoMap.keys.toSeq

  def listDataFrameNames(dsName: String): Seq[String] = {
    val url = getDataSetInfoMap.get(dsName).getOrElse(return Seq.empty)._3.url
    get(url).collect().map(row=>row.getAs[String](0))
  }

  def getDataSetMetaData(dsName: String): Model = {
    val model = ModelFactory.createDefaultModel()
    val rdfString = getDataSetInfoMap.get(dsName).getOrElse(return model)._1
    val reader = new StringReader(rdfString)
    model.read(reader, null, "RDF/XML")
    model
  }

  //TODO: Remove this method and update related test cases accordingly
  def getByPath(path: String): DataFrame = {
    super.get(dacpUrlPrefix + path)
  }

  def getDocument(dataFrameName: String): DataFrameDocument = {
    val jo = new JSONArray(getDataFrameInfoMap.get(dataFrameName).map(_._3).getOrElse(None)).getJSONObject(0)

    new DataFrameDocument {
      override def getSchemaURL(): Option[String] = Some("[SchemaURL defined by provider]")

      override def getColumnURL(colName: String): Option[String] = Some(jo.getString("url"))

      override def getColumnAlias(colName: String): Option[String] = Some(jo.getString("alias"))

      override def getColumnTitle(colName: String): Option[String] = Some(jo.getString("title"))
    }
  }

  def getStatistics(dataFrameName: String): DataFrameStatistics = {
    val jo = new JSONObject(getDataFrameInfoMap.get(dataFrameName).map(_._4).getOrElse(""))
    new DataFrameStatistics{
      override def rowCount: Long = jo.getLong("rowCount")
      override def byteSize: Long = jo.getLong("byteSize")
    }
  }

  def getDataFrameSize(dataFrameName: String): Long = {
    getDataFrameInfoMap.get(dataFrameName).map(_._1).getOrElse(0L)
  }

  def getHostInfo: Map[String, String] = {
    val jo = new JSONObject(getHostInfoMap().head._2._1)
    jo.keys().asScala.map { key =>
      key -> jo.getString(key)
    }.toMap
  }

  def getServerResourceInfo: Map[String, String] = {
    val jo = new JSONObject(getHostInfoMap().head._2._2)
    jo.keys().asScala.map { key =>
      key -> jo.getString(key)
    }.toMap
  }

  private val dacpUrlPrefix: String = s"$prefixSchema://$host:$port"

  //dataSetName -> (metaData, dataSetInfo, dataFrames)
  def  getDataSetInfoMap: Map[String, (String, String, DFRef)] = {
    val result = mutable.Map[String, (String, String, DFRef)]()
    get(dacpUrlPrefix+"/listDataSets").mapIterator(rows => rows.foreach(row => {
      result.put(row.getAs[String](0), (row.getAs[String](1), row.getAs[String](2), row.getAs[DFRef](3)))
    }))
    result.toMap
  }
  //dataFrameName -> (size,document,schema,statistic,dataFrame)
  def getDataFrameInfoMap: Map[String, (Long, String, String, String, DFRef)] = {
    val result = mutable.Map[String, (Long, String, String, String, DFRef)]()
    getDataSetInfoMap.values.map(v => get(v._3.url)).foreach(df => {
      df.mapIterator(iter => iter.foreach(row => {
        result.put(row.getAs[String](0), (row.getAs[Long](1), row.getAs[String](2), row.getAs[String](3), row.getAs[String](4), row.getAs[DFRef](5)))
      }))
    })
    result.toMap
  }
  //hostName -> (hostInfo, resourceInfo)
  def getHostInfoMap(): Map[String, (String, String)] = {
    val result = mutable.Map[String, (String, String)]()
    get(dacpUrlPrefix+"/listHostInfo").mapIterator(iter => iter.foreach(row => {
      result.put(row.getAs[String](0),(row.getAs[String](1), row.getAs[String](2)))
    }))
    result.toMap
  }
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
