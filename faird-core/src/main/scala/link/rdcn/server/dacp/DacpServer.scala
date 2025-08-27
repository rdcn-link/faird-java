package link.rdcn.server.dacp

import link.rdcn.ConfigKeys._
import link.rdcn.provider.DataProvider
import link.rdcn.received.DataReceiver
import link.rdcn.server.dftp.{ActionRequest, ActionResponse, DftpServer, DftpServiceHandler, GetRequest, GetResponse, PutRequest, PutResponse}
import link.rdcn.struct.ValueType._
import link.rdcn.struct.{DFRef, DataFrame, DataStreamSource, DefaultDataFrame, Row, StructType}
import link.rdcn.util.DataUtils
import link.rdcn.util.ServerUtils.getResourceStatusString
import link.rdcn.{ConfigLoader, FairdConfig}
import link.rdcn.client.UrlValidator
import link.rdcn.user.AuthProvider
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.{JSONArray, JSONObject}

import java.io.StringWriter
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 17:08
 * @Modified By:
 */

class DacpServer(dataProvider: DataProvider, dataReceiver: DataReceiver, authProvider: AuthProvider){

  val protocolSchema: String = "dacp"
  val dftpServiceHandler = new DftpServiceHandler {
    override def doAction(request: ActionRequest, response: ActionResponse): Unit = ???

    override def doGet(request: GetRequest, response: GetResponse): Unit = {
      val path = UrlValidator.extractPath(request.getRequestedUrl()) match {
        case Right(path) => path
        case Left(message) => throw new IllegalArgumentException(message)
      }
      val operation = request.getTransformer()
      path match {
        case path if path.startsWith("/get/") =>
          val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(path.stripPrefix("/get/"))
          val inDataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
          val outDataFrame: DataFrame  = operation.execute(inDataFrame)
          response.sendDataFrame(outDataFrame)
        case "/listDataSetNames/" =>
          response.sendDataFrame(operation.execute(doListDataSets()))
        case path if path.startsWith("/listDataFrames/") => {
          response.sendDataFrame(operation.execute(doListDataFrames(request.getRequestedUrl())))
        }
        case path if path.startsWith("/listHostInfo/") => {
          response.sendDataFrame(operation.execute(doListHostInfo))
        }
        case _ => response.sendError(400, s"bad request ${request.getRequestedUrl()}")
      }
    }

    override def doPut(request: PutRequest, response: PutResponse): Unit = {
      val dataFrame = request.getDataFrame()
      try{
        dataReceiver.start()
        dataReceiver.receiveRow(dataFrame)
        dataReceiver.finish()
        dataReceiver.close()
      }catch {
        case e: Exception => response.sendError(500, e.getMessage)
      }
      val jo = new JSONObject()
      response.sendJsonString(jo.put("status","success").toString)
    }
  }
  val server = new DftpServer
  server.setProtocolSchema(protocolSchema)
  server.setAuthHandler(authProvider)
  server.setDftpServiceHandler(dftpServiceHandler)

  def start(fairdConfig: FairdConfig): Unit = server.start(fairdConfig)

  def close(): Unit = server.close()
  /**
   * 输入链接（实现链接）： dacp://0.0.0.0:3101/listDataSets
   * 返回链接：
   *             dacp://0.0.0.0:3101/listDataFrames/dataSetName
   * */
  def doListDataSets(): DataFrame = {
    val stream = dataProvider.listDataSetNames().asScala.map(dsName => {
      val model: Model = ModelFactory.createDefaultModel
      dataProvider.getDataSetMetaData(dsName, model)
      val writer = new StringWriter();
      model.write(writer, "RDF/XML");
      val dataSetInfo = new JSONObject().put("name", dsName)
      Row.fromTuple((dsName, writer.toString
        ,dataSetInfo, DFRef(s"${server.url}/listDataFrames/$dsName")))
    }).toIterator
    val schema = StructType.empty.add("name", StringType)
      .add("meta", StringType).add("DataSetInfo", StringType).add("dataFrames", RefType)
    DefaultDataFrame(schema, stream)
  }

  /**
   * 输入链接（实现链接）： dacp://0.0.0.0:3101/listDataFrames/dataSetName
   * 返回链接： dacp://0.0.0.0:3101/get/dataFrameName
   * */
  def doListDataFrames(listDataFrameUrl: String): DataFrame = {
    val dataSetName = listDataFrameUrl.stripPrefix(server.url)
    val schema = StructType.empty.add("name", StringType).add("size", LongType)
      .add("document", StringType).add("schema", StringType).add("statistics", StringType)
      .add("dataFrame", RefType)
    val stream: Iterator[Row] =dataProvider.listDataFrameNames(dataSetName).asScala
      .map(dfName => {
        (dfName, dataProvider.getDataStreamSource(dfName).rowCount
          ,getDataFrameDocumentJsonString(dfName) , getDataFrameSchemaString(dfName)
          ,getDataFrameStatisticsString(dfName), DFRef(s"${server.url}/get/$dfName"))
      })
      .map(Row.fromTuple(_)).toIterator
    DefaultDataFrame(schema, stream)
  }

  /**
   * 输入链接(实现链接)： dacp://0.0.0.0:3101/getHost
   * */
  def doListHostInfo(): DataFrame = {
    val schema = StructType.empty.add("name", StringType).add("hostInfo", StringType).add("resourceInfo", StringType)
    val hostName = ConfigLoader.fairdConfig.hostName
    val stream = Seq((hostName, getHostInfoString(), getHostResourceString()))
      .map(Row.fromTuple(_)).toIterator
    DefaultDataFrame(schema, stream)
  }

  private def getHostInfoString(): String = {
    val hostInfo = Map(s"$FAIRD_HOST_NAME" -> s"${ConfigLoader.fairdConfig.hostName}",
      s"$FAIRD_HOST_TITLE" -> s"${ConfigLoader.fairdConfig.hostTitle}",
      s"$FAIRD_HOST_POSITION" -> s"${ConfigLoader.fairdConfig.hostPosition}",
      s"$FAIRD_HOST_DOMAIN" -> s"${ConfigLoader.fairdConfig.hostDomain}",
      s"$FAIRD_HOST_PORT" -> s"${ConfigLoader.fairdConfig.hostPort}",
      s"$FAIRD_TLS_ENABLED" -> s"${ConfigLoader.fairdConfig.useTLS}",
      s"$FAIRD_TLS_CERT_PATH" -> s"${ConfigLoader.fairdConfig.certPath}",
      s"$FAIRD_TLS_KEY_PATH" -> s"${ConfigLoader.fairdConfig.keyPath}",
      s"$LOGGING_FILE_NAME" -> s"${ConfigLoader.fairdConfig.loggingFileName}",
      s"$LOGGING_LEVEL_ROOT" -> s"${ConfigLoader.fairdConfig.loggingLevelRoot}",
      s"$LOGGING_PATTERN_CONSOLE" -> s"${ConfigLoader.fairdConfig.loggingPatternConsole}",
      s"$LOGGING_PATTERN_FILE" -> s"${ConfigLoader.fairdConfig.loggingPatternFile}")
    val jo = new JSONObject()
    hostInfo.foreach(kv => jo.put(kv._1, kv._2))
    jo.toString()
  }

  private def getHostResourceString(): String = {
    val jo = new JSONObject()
    getResourceStatusString.foreach(kv => jo.put(kv._1, kv._2))
    jo.toString()
  }

  private def getDataFrameDocumentJsonString(dataFrameName: String): String = {
    val document = dataProvider.getDocument(dataFrameName)
    val schema = StructType.empty.add("url", StringType).add("alias", StringType).add("title", StringType)
    val stream = getSchema(dataFrameName).columns.map(col => col.name).map(name => Seq(document.getColumnURL(name).getOrElse("")
        , document.getColumnAlias(name).getOrElse(""), document.getColumnTitle(name).getOrElse("")))
      .map(seq => link.rdcn.struct.Row.fromSeq(seq))
    val ja = new JSONArray()
    stream.map(_.toJsonObject(schema)).foreach(ja.put(_))
    ja.toString()
  }
  private def getDataFrameSchemaString(dataFrameName: String): String = {
    val structType = getSchema(dataFrameName)
    val schema = StructType.empty.add("name", StringType).add("valueType", StringType).add("nullable", StringType)
    val stream = structType.columns.map(col => Seq(col.name, col.colType.name, col.nullable.toString))
      .map(seq => Row.fromSeq(seq))
    val ja = new JSONArray()
    stream.map(_.toJsonObject(schema)).foreach(ja.put(_))
    ja.toString()
  }
  private def getDataFrameStatisticsString(dataFrameName: String): String = {
    val statistics = dataProvider.getStatistics(dataFrameName)
    val jo = new JSONObject()
    jo.put("byteSize", statistics.byteSize)
    jo.put("rowCount", statistics.rowCount)
    jo.toString()
  }

  private def getSchema(dataFrameName: String): StructType = {
    val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dataFrameName)
    var structType = dataStreamSource.schema
    if (structType.isEmpty()) {
      val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dataFrameName)
      val iter = dataStreamSource.iterator
      if (iter.hasNext) {
        structType = DataUtils.inferSchemaFromRow(iter.next())
      }
    }
    structType
  }
}
