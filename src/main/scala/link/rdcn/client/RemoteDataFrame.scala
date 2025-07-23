package link.rdcn.client

import link.rdcn.Logging
import link.rdcn.dftree._
import link.rdcn.provider.{DataFrameDocument, DataFrameStatistics}
import link.rdcn.struct.{DataFrame, Row, StructType}
import link.rdcn.util.{AutoClosingIterator, ResourceUtils}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 17:24
 * @Modified By:
 */

case class RemoteDataFrame(dataFrameName: String, client: ArrowFlightProtocolClient, operation: Operation = SourceOp()) extends DataFrame with Logging {
  val schema: StructType = StructType.fromString(getSchema)

  override def filter(f: Row => Boolean): DataFrame = {
    val genericFunctionCall = SingleRowCall(new SerializableFunction[Row, Boolean] {
      override def apply(v1: Row): Boolean = f(v1)
    })
    val filterOp = FilterOp(FunctionWrapper.getJavaSerialized(genericFunctionCall), operation)
    copy(operation = filterOp)
  }

  override def select(columns: String*): DataFrame = {
    copy(operation = SelectOp(operation, columns: _*))
  }

  override def limit(n: Int): DataFrame = copy(operation = LimitOp(n, operation))

  override def map(f: Row => Row): DataFrame = {
    val genericFunctionCall = SingleRowCall(new SerializableFunction[Row, Row] {
      override def apply(v1: Row): Row = f(v1)
    })
    val mapOperationNode = MapOp(FunctionWrapper.getJavaSerialized(genericFunctionCall), operation)
    copy(operation = mapOperationNode)
  }

  override def reduce(f: ((Row, Row)) => Row): DataFrame = ???

  def groupBy(column: String): GroupedDataFrame = ???

  override def foreach(f: Row => Unit): Unit = records.foreach(f)

  override def collect(): List[Row] = records.toList

  def getDocument: DataFrameDocument = client.getDocument(dataFrameName)

  def getStatistics: DataFrameStatistics = client.getStatistics(dataFrameName)

  private def records(): Iterator[Row] = client.getRows(dataFrameName, operation.toJsonString)

  private def getSchema: String = client.getSchema(dataFrameName)

  override def mapIterator[T](f: AutoClosingIterator[Row] => T): T = ResourceUtils.using(client.getRows(dataFrameName, operation.toJsonString)){f(_)}
}

case class GroupedDataFrame(remoteDataFrameImpl: RemoteDataFrame) {
  def max(column: String): RemoteDataFrame = ???
  //可自定义聚合函数
}



