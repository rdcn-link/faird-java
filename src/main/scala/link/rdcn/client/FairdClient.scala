package link.rdcn.client

import link.rdcn.client.dag.{DAGNode, SourceNode, TransformerDAG, UDFFunction}
import link.rdcn.dftree.{FunctionWrapper, Operation, SourceOp, TransformerNode}
import link.rdcn.struct.Row
import link.rdcn.user.{Credentials, UsernamePassword}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 14:49
 * @Modified By:
 */
private case class DacpUri(host: String, port: Int)

private object DacpUriParser {
  private val DacpPattern = "^dacp://([^:/]+):(\\d+)$".r

  def parse(uri: String): Either[String, DacpUri] = {
    uri match {
      case DacpPattern(host, portStr) =>
        try {
          val port = portStr.toInt
          if (port < 0 || port > 65535)
            Left(s"Invalid port number: $port")
          else
            Right(DacpUri(host, port))
        } catch {
          case _: NumberFormatException => Left(s"Invalid port format: $portStr")
        }

      case _ => Left(s"Invalid dacp URI format: $uri")
    }
  }
}

class FairdClient private (
                            url: String,
                            port: Int,
                            credentials: Credentials = Credentials.ANONYMOUS
                          ) {
  private val protocolClient = new ArrowFlightProtocolClient(url, port)
  protocolClient.login(credentials)

  def open(dataFrameName: String): RemoteDataFrameImpl =
    RemoteDataFrameImpl(dataFrameName, protocolClient)

  def listDataSetNames(): Seq[String] =
    protocolClient.listDataSetNames()

  def listDataFrameNames(dsName: String): Seq[String] =
    protocolClient.listDataFrameNames(dsName)

  def getDataSetMetaData(dsName: String): String =
    protocolClient.getDataSetMetaData(dsName)

  def getDataFrameSize(dataFrameName: String): Long =
    protocolClient.getDataFrameSize(dataFrameName)

  def getHostInfo(): String =
    protocolClient.getHostInfo()

  def getServerResourceInfo(): String =
    protocolClient.getServerResourceInfo()

  def close(): Unit = protocolClient.close()

  def execute(transformerDAG: TransformerDAG): Seq[RemoteDataFrame] = {
    val executePaths = transformerDAG.getExecutionPaths()
    executePaths.map(path => getRemoteDataFrameByDAGPath(path))
  }


  private def getRemoteDataFrameByDAGPath(path: Seq[DAGNode]): RemoteDataFrame = {
    val dataFrameName = path.head.asInstanceOf[SourceNode].dataFrameName
    var operation: Operation = SourceOp()
    path.foreach(node => node match {
      case f: UDFFunction =>
        val genericFunctionCall = IteratorRowCall( new SerializableFunction[Iterator[Row], Iterator[Row]] {
          override def apply(v1: Iterator[Row]): Iterator[Row] = f.transform(v1)
        })
        val transformerNode: TransformerNode = TransformerNode(FunctionWrapper.getJavaSerialized(genericFunctionCall), operation)
        operation = transformerNode
      case s: SourceNode => // 不做处理
      case _ => throw new IllegalArgumentException(s"This DAGNode ${node} is not supported please extend UDFFunction trait")
    })
    RemoteDataFrameImpl(dataFrameName, protocolClient, operation)
  }

}


object FairdClient {

  def connect(url: String, credentials: Credentials = Credentials.ANONYMOUS): FairdClient =
    DacpUriParser.parse(url) match {
      case Right(parsed) =>
        new FairdClient(parsed.host, parsed.port, credentials)
      case Left(err) =>
        throw new IllegalArgumentException(s"Invalid DACP URL: $err")
    }
}
