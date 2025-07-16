package link.rdcn.client

import link.rdcn.Logging
import link.rdcn.dftree.{FilterOp, FunctionWrapper, LimitOp, MapOp, Operation, SelectOp, SourceOp}
import link.rdcn.provider.DataFrameDocument
import link.rdcn.struct.Row

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 17:24
 * @Modified By:
 */
trait RemoteDataFrame{
  val dataFrameName: String
  val operation: Operation

  def getDataFrameDocument: DataFrameDocument

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

case class RemoteDataFrameImpl(dataFrameName: String, client: ArrowFlightProtocolClient, operation: Operation = SourceOp()) extends RemoteDataFrame with Logging {

  override def filter(f: Row => Boolean): RemoteDataFrame = {
    val genericFunctionCall = SingleRowCall( new SerializableFunction[Row, Boolean] {
      override def apply(v1: Row): Boolean = f(v1)
    })
    val filterOp = FilterOp(FunctionWrapper.getJavaSerialized(genericFunctionCall), operation)
    copy(operation = filterOp)
  }

  override def select(columns: String*): RemoteDataFrame = {
    copy(operation =  SelectOp(operation, columns: _*))
  }

  override def limit(n: Int): RemoteDataFrame = copy(operation = LimitOp(n, operation))

  override def map(f: Row => Row): RemoteDataFrame = {
    val genericFunctionCall = SingleRowCall( new SerializableFunction[Row, Row] {
      override def apply(v1: Row): Row = f(v1)
    })
    val mapOperationNoe = MapOp(FunctionWrapper.getJavaSerialized(genericFunctionCall), operation)
    copy(operation = mapOperationNoe)
  }

  override def reduce(f: ((Row, Row)) => Row): RemoteDataFrame = ???

  def groupBy(column: String): GroupedDataFrame = ???

  override def foreach(f: Row => Unit): Unit = records.foreach(f)

  override def collect(): List[Row] = records.toList

  override def getDataFrameDocument: DataFrameDocument = client.getDataFrameDocument(dataFrameName)

  private def records(): Iterator[Row] = client.getRows(dataFrameName, operation.toJsonString)
}



