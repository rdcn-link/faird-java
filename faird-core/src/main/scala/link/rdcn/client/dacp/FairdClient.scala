package link.rdcn.client.dacp

import link.rdcn.client.UrlValidator
import link.rdcn.client.dag.Flow
import link.rdcn.client.dftp.DftpClient
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics}
import link.rdcn.struct.{DFRef, DataFrame, ExecutionResult}
import link.rdcn.user.Credentials
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.io.StringReader
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/18 16:30
 * @Modified By:
 */
class FairdClient private(url: String, port: Int, useTLS: Boolean = false) extends DftpClient(url, port, useTLS) {

  override val prefixSchema: String = "dacp"
  def listDataSetNames(): Seq[String] = getDataSetInfoMap.keys.toSeq

  def listDataFrameNames(dsName: String): Seq[String] = getDataFrameInfoMap.keys.toSeq

  def getDataSetMetaData(dsName: String): Model = {
    val model = ModelFactory.createDefaultModel()
    val rdfString = getDataSetInfoMap.get(dsName).getOrElse(return model)._1
    val reader = new StringReader(rdfString)
    model.read(reader, null, "RDF/XML")
    model
  }

  override def get(path: String): DataFrame = {
    super.get(dacpUrlPrefix + path)
  }

  def getDocument(dataFrameName: String): DataFrameDocument = ???

  def getStatistics(dataFrameName: String): DataFrameStatistics = ???

  def getDataFrameSize(dataFrameName: String): Long = ???

  def getHostInfo: Map[String, String] = ???

  def getServerResourceInfo: Map[String, String] = ???

  private val dacpUrlPrefix: String = s"$prefixSchema://$url:$port"

  //dataSetName -> (metaData, dataSetInfo, dataFrames)
  private def  getDataSetInfoMap(): Map[String, (String, String, DFRef)] = {
    val result = mutable.Map[String, (String, String, DFRef)]()
    get("/listDataSets").mapIterator(rows => rows.foreach(row => {
      result.put(row.getAs[String](0), (row.getAs[String](1), row.getAs[String](2), row.getAs[DFRef](3)))
    }))
    result.toMap
  }
  //dataFrameName -> (size,document,schema,statistic,dataFrame)
  private def getDataFrameInfoMap: Map[String, (Long, String, String, String, DFRef)] = {
    val result = mutable.Map[String, (Long, String, String, String, DFRef)]()
    getDataSetInfoMap.keys.map(dsName => get(s"/listDataFrames/$dsName")).foreach(df => {
      df.mapIterator(iter => iter.foreach(row => {
        result.put(row.getAs[String](0), (row.getAs[Long](1), row.getAs[String](2), row.getAs[String](3), row.getAs[String](4), row.getAs[DFRef](5)))
      }))
    })
    result.toMap
  }
  private def getHostInfoMap(): Map[String, (String, String)] = {
    val result = mutable.Map[String, (String, String)]()
    get("/listHostInfo").mapIterator(iter => iter.foreach(row => {
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
