package link.rdcn.client.union

import link.rdcn.client.UrlValidator
import link.rdcn.client.dacp.FairdClient
import link.rdcn.client.dag.{Flow, FlowNode, SourceNode}
import link.rdcn.client.dftp.DftpClient
import link.rdcn.struct.{DFRef, DataFrame, ExecutionResult}
import link.rdcn.user.Credentials

import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/28 09:23
 * @Modified By:
 */
private class UnionClient(host: String, port: Int, useTLS: Boolean = false) extends FairdClient(host, port, useTLS){

  private val endpoints = mutable.ListBuffer[Endpoint]()
  private val endpointClientsMap = mutable.Map[String, DftpClient]()

  def addEndpoint(endpoints: Endpoint*): Unit = {
    endpoints.foreach(endpoint => {
      this.endpointClientsMap.put(endpoint.url, endpoint.getDftpClient())
      this.endpoints.append(endpoint)
    })
  }

  override def get(url: String): DataFrame = {
    val baseUrl = UrlValidator.extractBase(url) match {
      case Some((baseUrl , _ ,_)) => baseUrl
      case None => throw new IllegalArgumentException(s"Invalid URL $url")
    }
    val dacpClient = endpointClientsMap.getOrElse(baseUrl, throw new IllegalArgumentException(s"No dacpClient available for the requested URL $url"))
    dacpClient.get(url)
  }

  override def execute(transformerDAG: Flow): ExecutionResult = {
    val executePaths: Seq[Seq[FlowNode]] = transformerDAG.getExecutionPaths()
    val dataFrameInfoMap = getDataFrameInfoMap
    val dfs = executePaths.map(flowNodes => {
      val sourceNodeUrl = dataFrameInfoMap.getOrElse(flowNodes.head.asInstanceOf[SourceNode].dataFrameName, throw new Exception(s"DataFrame not found for DAG SourceNode ${flowNodes.head}"))._5.url
      val sourceNodeClient = endpointClientsMap.getOrElse(UrlValidator.extractBase(sourceNodeUrl).get._1, throw new IllegalArgumentException(s"No dacpClient available for the requested URL $sourceNodeUrl"))
      sourceNodeClient.execute(Flow.pipe(flowNodes.head, flowNodes.tail: _*))
    }).map(_.single())

    new ExecutionResult() {
      override def single(): DataFrame = dfs.head

      override def get(name: String): DataFrame = dfs(name.toInt-1)

      override def map(): Map[String, DataFrame] = dfs.zipWithIndex.map {
        case (dataFrame, id) => (id.toString, dataFrame)
      }.toMap
    }
  }

  override def getDataSetInfoMap(): Map[String, (String, String, DFRef)] = {
    val result = mutable.Map[String, (String, String, DFRef)]()
    endpoints.foreach(endpoint => {
      get(endpoint.url+"/listDataSets").mapIterator(rows => rows.foreach(row => {
        result.put(row.getAs[String](0), (row.getAs[String](1), row.getAs[String](2), row.getAs[DFRef](3)))
      }))
    })
    result.toMap
  }

  override def getHostInfoMap(): Map[String, (String, String)] = {
    val result = mutable.Map[String, (String, String)]()
    endpoints.foreach(endpoint => {
      get(endpoint.url+"/listHostInfo").mapIterator(iter => iter.foreach(row => {
        result.put(row.getAs[String](0),(row.getAs[String](1), row.getAs[String](2)))
      }))
    })
    result.toMap
  }
}

object UnionClient {
  def connect(endpoints: Seq[Endpoint]): Unit = {
    val unionClient = new UnionClient("",0)
    unionClient.addEndpoint(endpoints :_*)
  }
}

case class Endpoint(
                     name: String,
                     host: String,
                     port: Int,
                     useTLS: Boolean = false,
                     credentials: Credentials = Credentials.ANONYMOUS
                   ){
  def url: String = s"${FairdClient.protocolSchema}://${host}:${port}"
  def getDftpClient(): DftpClient = {
    if(useTLS)
      FairdClient.connectTLS(url,credentials)
    else
      FairdClient.connect(url,credentials)
  }
}
