package link.rdcn.server

import io.circe.generic.auto._
import io.circe.parser._
import link.rdcn.ConfigKeys.{FAIRD_HOST_DOMAIN, FAIRD_HOST_NAME, FAIRD_HOST_PORT, FAIRD_HOST_POSITION, FAIRD_HOST_TITLE, FAIRD_TLS_CERT_PATH, FAIRD_TLS_ENABLED, FAIRD_TLS_KEY_PATH, LOGGING_FILE_NAME, LOGGING_LEVEL_ROOT, LOGGING_PATTERN_CONSOLE, LOGGING_PATTERN_FILE}
import link.rdcn.{ConfigLoader, SimpleSerializer}
import link.rdcn.ErrorCode.{INVALID_CREDENTIALS, USER_NOT_LOGGED_IN}
import link.rdcn.dftree.Operation
import link.rdcn.provider.DataProvider
import link.rdcn.received.DataReceiver
import link.rdcn.server.ActionType.GET
import link.rdcn.server.exception.{AuthorizationException, DataFrameAccessDeniedException}
import link.rdcn.struct.{DataFrame, DataStreamSource, DefaultDataFrame}
import link.rdcn.user.{AuthProvider, AuthenticatedUser, DataOperationType, UsernamePassword}
import link.rdcn.util.ServerUtils.{getArrayBytesStream, getListStringStream, getResourceStatusString, getSingleLongBytesStream, getSingleStringStream}
import link.rdcn.util.{DataUtils, ServerUtils}
import org.apache.arrow.vector.VarCharVector
import org.apache.jena.rdf.model.{Model, ModelFactory}

import java.io.StringWriter
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/17 17:08
 * @Modified By:
 */
class DacpServer(dataProvider: DataProvider, authProvider: AuthProvider, dataReceiver: DataReceiver, fairdHome: String) extends DftpServer(fairdHome: String) {

  private val requestMap = new ConcurrentHashMap[String, (String, Operation)]()
  private val authenticatedUserMap = new ConcurrentHashMap[String, AuthenticatedUser]()

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
          val out: StringWriter  = new StringWriter()
          model.write(out, "TTL")
          Some(DataFrame.fromSeq(Seq(out.toString)))
        }
        case actionType if actionType.startsWith("/getDataFrameSize") => {
          val dfName =  actionType.stripPrefix("/getDataFrameSize/")
          val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dfName)
          Some(DataFrame.fromSeq(Seq(dataStreamSource.rowCount)))
        }

        case actionType if actionType.startsWith("/getHostInfo") =>
          val hostInfo =
            s"""
             {"$FAIRD_HOST_NAME": "${ConfigLoader.fairdConfig.hostName}",
             "$FAIRD_HOST_TITLE": "${ConfigLoader.fairdConfig.hostTitle}",
             "$FAIRD_HOST_POSITION": "${ConfigLoader.fairdConfig.hostPosition}",
             "$FAIRD_HOST_DOMAIN": "${ConfigLoader.fairdConfig.hostDomain}",
             "$FAIRD_HOST_PORT": "${ConfigLoader.fairdConfig.hostPort}",
             "$FAIRD_TLS_ENABLED": "${ConfigLoader.fairdConfig.useTLS}",
             "$FAIRD_TLS_CERT_PATH": "${ConfigLoader.fairdConfig.certPath}",
             "$FAIRD_TLS_KEY_PATH": "${ConfigLoader.fairdConfig.keyPath}",
             "$LOGGING_FILE_NAME": "${ConfigLoader.fairdConfig.loggingFileName}",
             "$LOGGING_LEVEL_ROOT": "${ConfigLoader.fairdConfig.loggingLevelRoot}",
             "$LOGGING_PATTERN_CONSOLE": "${ConfigLoader.fairdConfig.loggingPatternConsole}",
             "$LOGGING_PATTERN_FILE": "${ConfigLoader.fairdConfig.loggingPatternFile}"
             }""".stripMargin.replaceAll("\n", "").replaceAll("\\s+", " ")
          Some(DataFrame.fromSeq(Seq(hostInfo)))

        case actionType if actionType.startsWith("/getServerResourceInfo") =>
          Some(DataFrame.fromSeq(Seq(getResourceStatusString)))
        case actionType if actionType.startsWith("/getDocument") => {
          val dfName = actionType.stripPrefix("/getDocument/")
          val dataFrameDocumentBytes = SimpleSerializer.serialize(dataProvider.getDocument(dfName))
          Some(DataFrame.fromSeq(Seq(dataFrameDocumentBytes)))
        }
        case actionType if actionType.startsWith("/getSchema/") => {
          val dfName =  actionType.stripPrefix("/getSchema/")
          val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dfName)
          var structType = dataStreamSource.schema
          if(structType.isEmpty()){
            val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(dfName)
            val iter = dataStreamSource.iterator
            if(iter.hasNext){
              structType = DataUtils.inferSchemaFromRow(iter.next())
            }
          }
          Some(DataFrame.fromSeq(Seq(structType.toString)))
        }
        case actionType if actionType.startsWith("/getStatistics/") => {
          val dfName =  actionType.stripPrefix("/getStatistics/")
          val dataFrameStatisticsBytes: Array[Byte] = SimpleSerializer.serialize(dataProvider.getStatistics(dfName))
          Some(DataFrame.fromSeq(Seq(dataFrameStatisticsBytes)))
        }
        case actionType if actionType.startsWith("login") =>
          val childAllocator = allocator.newChildAllocator("flight-session", 0, Long.MaxValue)
          val root = ServerUtils.getVectorSchemaRootFromBytes(request.body,childAllocator)
          val credentialsJsonString: String = root.getFieldVectors.get(0).asInstanceOf[VarCharVector].getObject(0).toString
          val credentials = decode[UsernamePassword](credentialsJsonString)
          credentials match {
            case Left(_) => throw new AuthorizationException(INVALID_CREDENTIALS)
            case Right(usernamePassword: UsernamePassword) =>
              val authenticatedUser: AuthenticatedUser = authProvider.authenticate(usernamePassword)
              val loginToken: String = actionType.split("\\.")(1)
              authenticatedUserMap.put(loginToken, authenticatedUser)
          }
          None
        case actionType if actionType.startsWith("putRequest") =>
          val dfName =  actionType.split(":")(1)
          val userToken =  actionType.split(":")(2)
          val authenticatedUser = Option(authenticatedUserMap.get(userToken))
          if(authenticatedUser.isEmpty){
            throw new AuthorizationException(USER_NOT_LOGGED_IN)
          }
          if(! authProvider.checkPermission(authenticatedUser.get, dfName, List.empty[DataOperationType].asJava.asInstanceOf[java.util.List[DataOperationType]] ))
            throw new DataFrameAccessDeniedException(dfName)
          val operationNode: Operation = Operation.fromJsonString(new String(request.body, StandardCharsets.UTF_8))
          requestMap.put(userToken, (dfName, operationNode))
          None
        case ticket =>
          //TODO 重新构造ticket
          val request = requestMap.get(ticket)
          val dataStreamSource: DataStreamSource = dataProvider.getDataStreamSource(request._1)
          val inDataFrame = DefaultDataFrame(dataStreamSource.schema, dataStreamSource.iterator)
          val outDataFrame: DataFrame  = request._2.execute(inDataFrame)

          Option(outDataFrame)
      }
      response.set(200, df)
    }
  }

  override def doPut(request: DftpRequest, response: DftpResponse): Unit = ???
}
