package link.rdcn.client

import link.rdcn.{Logging, dftree}
import link.rdcn.dftree.{FunctionWrapper, LangType, OperationNode, OperationType, SourceNode, TransformNode}
import link.rdcn.struct.Row

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 17:24
 * @Modified By:
 */
trait SerializableFunction[-T, +R] extends (T => R) with Serializable

trait RemoteDataFrame{
  val dataFrameName: String
  val operationNode: OperationNode

  def getSchema: String

  def getSchemaURI: String

  def map(f: Row => Row): RemoteDataFrame

  def filter(f: Row => Boolean): RemoteDataFrame

  def select(columns: String*): RemoteDataFrame

  def limit(n: Int): RemoteDataFrame

  def reduce(f: ((Row, Row)) => Row): RemoteDataFrame

  def foreach(f: Row => Unit): Unit // 远程调用 + 拉取结果

  def collect(): List[Row]
}

case class GroupedDataFrame(remoteDataFrameImpl: RemoteDataFrameImpl) {
  def max(column: String): RemoteDataFrameImpl = ???
  //可自定义聚合函数
}

case class RemoteDataFrameImpl(dataFrameName: String, client: ArrowFlightProtocolClient, operationNode: OperationNode = SourceNode()) extends RemoteDataFrame with Logging {

  override def filter(f: Row => Boolean): RemoteDataFrame = {
    val filterOp = FilterOp(new SerializableFunction[Row, Boolean] {
      override def apply(v1: Row): Boolean = f(v1)
    })
    val filterOperationNode = TransformNode(OperationType.Filter, LangType.JAVA_BIN, FunctionWrapper.getJavaSerialized(filterOp), operationNode)
    copy(operationNode = filterOperationNode)
  }

  override def select(columns: String*): RemoteDataFrame = {
    val selectOperationNode = TransformNode(OperationType.Select, LangType.JAVA_BIN, FunctionWrapper.getJavaSerialized(SelectOp(columns)), operationNode)
    copy(operationNode = selectOperationNode)
  }

  override def limit(n: Int): RemoteDataFrame = {
    val selectOperationNode = TransformNode(OperationType.Limit, LangType.JAVA_BIN, FunctionWrapper.getJavaSerialized(LimitOp(n)), operationNode)
    copy(operationNode = selectOperationNode)
  }

  override def map(f: Row => Row): RemoteDataFrame = {
    val mapOp = MapOp(new SerializableFunction[Row, Row] {
      override def apply(v1: Row): Row = f(v1)
    })
    val mapOperationNoe = TransformNode(OperationType.Map, LangType.JAVA_BIN, FunctionWrapper.getJavaSerialized(mapOp), operationNode)
    copy(operationNode = mapOperationNoe)
  }

  override def reduce(f: ((Row, Row)) => Row): RemoteDataFrame = {
    val reduceOp = ReduceOp(new SerializableFunction[(Row, Row), Row] {
      override def apply(v1: (Row, Row)): Row = f(v1)
    })
    val reduceOperationNode = TransformNode(OperationType.Reduce, LangType.JAVA_BIN, FunctionWrapper.getJavaSerialized(reduceOp), operationNode)
    copy(operationNode = reduceOperationNode)
  }

  def groupBy(column: String): GroupedDataFrame = ???

  override def foreach(f: Row => Unit): Unit = records.foreach(f)

  override def collect(): List[Row] = records.toList

  override def getSchema: String = client.getSchema(dataFrameName)

  override def getSchemaURI: String = client.getSchemaURI(dataFrameName)

  private def records(): Iterator[Row] = client.getRows(dataFrameName, operationNode.toJsonString)
}



