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
import link.rdcn.Logging
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

class DacpServer(dataProvider: DataProvider, dataReceiver: DataReceiver, authProvider: AuthProvider) extends Logging {

  val dftpServiceHandler = new DftpServiceHandler {
    override def doAction(request: ActionRequest, response: ActionResponse): Unit = response.sendError(501, s"${request.getActionName()} Not Implemented")

    override def doGet(request: GetRequest, response: GetResponse): Unit = this.doGet(request, response)

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
      response.sendMessage(jo.put("status","success").toString)
    }
  }
  val protocolSchema = "dacp"
  val server = new DftpServer().setProtocolSchema(protocolSchema)
    .setAuthHandler(authProvider)
    .setDftpServiceHandler(dftpServiceHandler)

  def start(fairdConfig: FairdConfig): Unit = {
    baseUrl = s"$protocolSchema://${fairdConfig.hostPosition}:${fairdConfig.hostPort}"
    ConfigLoader.init(fairdConfig)
    server.start(fairdConfig)
  }
  def close(): Unit = server.close()

  def doGet(request: GetRequest, response: GetResponse): Unit = {
    request.getRequestedPath() match {
      case "/listDataSets" =>
        try {
          response.sendDataFrame(doListDataSets())
        }catch {
          case e: Exception =>
            logger.error("Error while listDataSets", e)
            response.sendError(500, e.getMessage)
        }
      case path if path.startsWith("/listDataFrames") => {
        try{
          response.sendDataFrame(doListDataFrames(request.getRequestedPath()))
        }catch {
          case e: Exception =>
            logger.error("Error while listDataFrames", e)
            response.sendError(500, e.getMessage)
        }
      }
      case path if path.startsWith("/listHostInfo") => {
        try{
          response.sendDataFrame(doListHostInfo)
        }catch {
          case e: Exception =>
            logger.error("Error while listHostInfo", e)
            response.sendError(500, e.getMessage)
        }
      }
      case otherPath =>
        try{
          val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(otherPath)
          val dataFrame: DataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
          response.sendDataFrame(dataFrame)
        }catch {
          case e: Exception =>
            logger.error(s"Error while get resource $otherPath", e)
            response.sendError(500, e.getMessage)
        }
    }
  }

  /**
   * 输入链接（实现链接）： dacp://0.0.0.0:3101/listDataSets
   * 返回链接： dacp://0.0.0.0:3101/listDataFrames/dataSetName
   * */
  def doListDataSets(): DataFrame = {
    val stream = dataProvider.listDataSetNames().asScala.map(dsName => {
      val model: Model = ModelFactory.createDefaultModel
      dataProvider.getDataSetMetaData(dsName, model)
      val writer = new StringWriter();
      model.write(writer, "RDF/XML");
      val dataSetInfo = new JSONObject().put("name", dsName).toString
      Row.fromTuple((dsName, writer.toString
        ,dataSetInfo, DFRef(s"${baseUrl}/listDataFrames/$dsName")))
    }).toIterator
    val schema = StructType.empty.add("name", StringType)
      .add("meta", StringType).add("DataSetInfo", StringType).add("dataFrames", RefType)
    DefaultDataFrame(schema, stream)
  }

  /**
   * 输入链接（实现链接）： dacp://0.0.0.0:3101/listDataFrames/dataSetName
   * 返回链接： dacp://0.0.0.0:3101/dataFrameName
   * */
  def doListDataFrames(listDataFrameUrl: String): DataFrame = {
    val dataSetName = listDataFrameUrl.stripPrefix("/listDataFrames/")
    val schema = StructType.empty.add("name", StringType).add("size", LongType)
      .add("document", StringType).add("schema", StringType).add("statistics", StringType)
      .add("dataFrame", RefType)
    val stream: Iterator[Row] =dataProvider.listDataFrameNames(dataSetName).asScala
      .map(dfName => {
        (dfName, dataProvider.getDataStreamSource(dfName).rowCount
          ,getDataFrameDocumentJsonString(dfName) , getDataFrameSchemaString(dfName)
          ,getDataFrameStatisticsString(dfName), DFRef(s"${baseUrl}/$dfName"))
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

  var baseUrl: String = _

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
