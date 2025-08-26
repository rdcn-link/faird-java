package link.rdcn.client.dacp

import link.rdcn.client.dag._
import link.rdcn.client.{DataFrameCall, RemoteDataFrameProxy, SerializableFunction}
import link.rdcn.dftree.FunctionWrapper.RepositoryOperator
import link.rdcn.dftree._
import link.rdcn.struct.{DataFrame, ExecutionResult}
import link.rdcn.user.Credentials
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.json.JSONObject

import java.io.StringReader

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
  private val dacpClient = new DacpClient(url, port, useTLS)
  dacpClient.login(credentials)
  private val dacpUrlPrefix: String = s"dacp://$url:$port"

  def get(dataFrameName: String): RemoteDataFrameProxy =
    RemoteDataFrameProxy(dataFrameName, dacpClient)

  def listDataSetNames(): Seq[String] =
    dacpClient.get(dacpUrlPrefix + "/listDataSetNames").mapIterator[Seq[String]](iter =>{
      iter.map(row => row.getAs[String](0)).toSeq
    })

  def listDataFrameNames(dsName: String): Seq[String] =
    dacpClient.get(dacpUrlPrefix + s"/listDataFrameNames/$dsName").mapIterator[Seq[String]](iter =>{
      iter.map(row => row.getAs[String](0)).toSeq
    })

  def getDataSetMetaData(dsName: String): Model = {
    val rdfString = dacpClient.get(dacpUrlPrefix + s"/getDataSetMetaData/$dsName").collect().head.getAs[String](0)
    val model = ModelFactory.createDefaultModel()
    val reader = new StringReader(rdfString)
    model.read(reader, null, "RDF/XML")
    model
  }


  def getDataFrameSize(dataFrameName: String): Long =
    dacpClient.get(dacpUrlPrefix + s"/getDataFrameSize/$dataFrameName").collect().head.getAs[Long](0)

  def getHostInfo: Map[String, String] = {
    val df = dacpClient.get(dacpUrlPrefix + s"/getHostInfo")
    val schema = df.schema.columns
    schema.zip(df.collect().head.toSeq).map(kv => (kv._1.name, kv._2.toString)).toMap
  }

  def getServerResourceInfo: Map[String, String] = {
    val df = dacpClient.get(dacpUrlPrefix + s"/getServerResourceInfo")
    val schema = df.schema.columns
    schema.zip(df.collect().head.toSeq).map(kv => (kv._1.name, kv._2.toString)).toMap
  }

  def put(dataFrame: DataFrame): Unit = dacpClient.put(dataFrame)

  def close(): Unit = dacpClient.close()

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
    RemoteDataFrameProxy(dataFrameName, dacpClient, operation)
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
