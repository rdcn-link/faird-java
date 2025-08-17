package link.rdcn.client

import link.rdcn.client.dag._
import link.rdcn.dftree.FunctionWrapper.RepositoryOperator
import link.rdcn.dftree._
import link.rdcn.struct.{DataFrame, ExecutionResult}
import link.rdcn.user.Credentials
import org.apache.jena.rdf.model.Model
import org.json.JSONObject

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 14:49
 * @Modified By:
 */
class FairdClient private(
                           url: String,
                           port: Int,
                           credentials: Credentials = Credentials.ANONYMOUS,
                           useTLS: Boolean = false
                         ) {
  private val protocolClient = new ArrowFlightProtocolClient(url, port, useTLS)
  protocolClient.login(credentials)

  def get(dataFrameName: String): RemoteDataFrameProxy =
    RemoteDataFrameProxy(dataFrameName, protocolClient)

  def listDataSetNames(): Seq[String] =
    protocolClient.listDataSetNames()

  def listDataFrameNames(dsName: String): Seq[String] =
    protocolClient.listDataFrameNames(dsName)

  def getDataSetMetaData(dsName: String): Model =
    protocolClient.getDataSetMetaData(dsName)

  def getDataFrameSize(dataFrameName: String): Long =
    protocolClient.getDataFrameSize(dataFrameName)

  def getHostInfo: Map[String, String] =
    protocolClient.getHostInfo

  def getServerResourceInfo: Map[String, String] =
    protocolClient.getServerResourceInfo

  def put(dataFrame: DataFrame): Unit =
    protocolClient.put(dataFrame)

  def close(): Unit = protocolClient.close()

  def execute(transformerDAG: Flow): ExecutionResult = {
    val executePaths = transformerDAG.getExecutionPaths()
    val dfs: Seq[DataFrame] = executePaths.map(path => getRemoteDataFrameByDAGPath(path))
    new ExecutionResult() {
      override def single(): DataFrame = dfs.head

      override def get(name: String): DataFrame = dfs(name.toInt-1)

      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
        case (dataFrame, id) => (id.toString, dataFrame)
      }.toMap
    }
  }


  private def getRemoteDataFrameByDAGPath(path: Seq[FlowNode]): DataFrame = {
    val dataFrameName = path.head.asInstanceOf[SourceNode].dataFrameName
    var operation: Operation = SourceOp()
    path.foreach(node => node match {
      case f: Transformer11 =>
        val genericFunctionCall = DataFrameCall(new SerializableFunction[DataFrame, DataFrame] {
          override def apply(v1: DataFrame): DataFrame = f.transform(v1)
        })
        val transformerNode: TransformerNode = TransformerNode(FunctionWrapper.getJavaSerialized(genericFunctionCall), operation)
        operation = transformerNode
      case node: RepositoryNode =>
        val jo = new JSONObject()
        jo.put("type", LangType.REPOSITORY_OPERATOR.name)
        jo.put("functionID", node.functionId)
        val transformerNode: TransformerNode = TransformerNode(FunctionWrapper(jo).asInstanceOf[RepositoryOperator], operation)
        operation = transformerNode
      case s: SourceNode => // 不做处理
      case _ => throw new IllegalArgumentException(s"This FlowNode ${node} is not supported please extend Transformer11 trait")
    })
    RemoteDataFrameProxy(dataFrameName, protocolClient, operation)
  }

}

object FairdClient {

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): FairdClient = {
    DacpUrlValidator.validate(url) match {
      case Right(parsed) =>
        new FairdClient(parsed._1, parsed._2.getOrElse(3101), credentials)
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }


  def connectTLS(url: String, credentials: Credentials = Credentials.ANONYMOUS): FairdClient = {
    DacpUrlValidator.validate(url) match {
      case Right(parsed) =>
        new FairdClient(parsed._1, parsed._2.getOrElse(3101), credentials, true)
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
  }
}
