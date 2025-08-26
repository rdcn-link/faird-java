package link.rdcn.server.dacp

import link.rdcn.ConfigKeys._
import link.rdcn.provider.DataProvider
import link.rdcn.received.DataReceiver
import link.rdcn.server.ActionType.GET
import link.rdcn.server.{DftpRequest, DftpResponse, DftpServer}
import link.rdcn.struct.ValueType.StringType
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame, StructType, Row}
import link.rdcn.util.{ClosableIterator, DataUtils, ServerUtils}
import link.rdcn.util.ServerUtils.getResourceStatusString
import link.rdcn.{ConfigLoader, FairdConfig, SimpleSerializer}
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.io.StringWriter
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 17:08
 * @Modified By:
 */

class DacpServer(dataProvider: DataProvider, dataReceiver: DataReceiver) extends DftpServer {

  override def doGet(request: DftpRequest, response: DftpResponse): Unit = {
    if(request.actionType == GET){
      val df: Option[DataFrame] =  request.path match {
        case "/listDataSetNames" =>
          Some(DataFrame.fromSeq(dataProvider.listDataSetNames().asScala))
        case actionType if actionType.startsWith("/listDataFrameNames") => {
          val dataSet = actionType.stripPrefix("/listDataFrameNames/")
          Some(DataFrame.fromSeq(dataProvider.listDataFrameNames(dataSet).asScala))
        }
        case actionType if actionType.startsWith("/getDataSetMetaData") => {
          val dsName = actionType.stripPrefix("/getDataSetMetaData/")
          val model: Model = ModelFactory.createDefaultModel
          dataProvider.getDataSetMetaData(dsName, model)
          val writer = new StringWriter();
          model.write(writer, "RDF/XML");
          Some(DataFrame.fromSeq(Seq(writer.toString)))
        }
        case actionType if actionType.startsWith("/getDataFrameSize") => {
          val dfName =  actionType.stripPrefix("/getDataFrameSize/")
          val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dfName)
          Some(DataFrame.fromSeq(Seq(dataStreamSource.rowCount)))
        }

        case actionType if actionType.startsWith("/getHostInfo") =>
          val hostInfo =
            Map(s"$FAIRD_HOST_NAME" -> s"${ConfigLoader.fairdConfig.hostName}",
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

          Some(DataFrame.fromMap(Seq(hostInfo)))

        case actionType if actionType.startsWith("/getServerResourceInfo") =>
          Some(DataFrame.fromMap(Seq(getResourceStatusString)))
        case actionType if actionType.startsWith("/getDocument") => {
          val dfName = actionType.stripPrefix("/getDocument/")
          val document = dataProvider.getDocument(dfName)
          val schema = StructType.empty.add("url", StringType).add("alias", StringType).add("title", StringType)
          val stream = getSchema(dfName).columns.map(col => col.name).map(name => Seq(document.getColumnURL(name).getOrElse("")
              , document.getColumnAlias(name).getOrElse(""), document.getColumnTitle(name).getOrElse("")))
            .map(seq => link.rdcn.struct.Row.fromSeq(seq))
          Some(DefaultDataFrame(schema, ClosableIterator(stream.toIterator)()))
        }
        case actionType if actionType.startsWith("/getSchema/") => {
          val dfName =  actionType.stripPrefix("/getSchema/")
          val structType = getSchema(dfName)
          val schema = StructType.empty.add("name", StringType).add("valueType", StringType).add("nullable", StringType)
          val stream = structType.columns.map(col => Seq(col.name, col.colType.name, col.nullable.toString))
            .map(seq => Row.fromSeq(seq)).toIterator
          Some(DefaultDataFrame(schema, ClosableIterator(stream)()))
        }
        case actionType if actionType.startsWith("/getStatistics/") => {
          val dfName =  actionType.stripPrefix("/getStatistics/")
          val dataFrameStatisticsBytes: Array[Byte] = SimpleSerializer.serialize(dataProvider.getStatistics(dfName))
          Some(DataFrame.fromSeq(Seq(dataFrameStatisticsBytes)))
        }
        case actionType if actionType.startsWith("/get") =>
          val request = requestMap.get(actionType.stripPrefix("/get/"))
          val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(request._1)
          val inDataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
          val outDataFrame: DataFrame  = request._2.execute(inDataFrame)
          Option(outDataFrame)
      }
      response.send(200, df)
    }
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

  override def doPut(request: DftpRequest, response: DftpResponse): Unit = ???
}
