package link.rdcn.client

import link.rdcn.Logging
import link.rdcn.client.dag.{SerializableFunction, SingleRowCall}
import link.rdcn.dftree._
import link.rdcn.struct.{DataFrame, Row, StructType}
import link.rdcn.util.{ClosableIterator, ResourceUtils}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 17:24
 * @Modified By:
 */

case class RemoteDataFrameProxy(dataFrameName: String, getRows: (String, String) => (StructType,ClosableIterator[Row]), operation: Operation = SourceOp()) extends DataFrame with Logging {

  override val schema: StructType = getRows(dataFrameName, operation.toJsonString)._1

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

  private def records(): Iterator[Row] = getRows(dataFrameName, operation.toJsonString)._2

  override def mapIterator[T](f: ClosableIterator[Row] => T): T = ResourceUtils.using(getRows(dataFrameName, operation.toJsonString)._2){f(_)}
}

case class GroupedDataFrame(remoteDataFrameImpl: RemoteDataFrameProxy) {
  def max(column: String): RemoteDataFrameProxy = ???
  //可自定义聚合函数
}



