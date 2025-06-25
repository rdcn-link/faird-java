package link.rdcn.client

import link.rdcn.Logging
import link.rdcn.struct.Row
import org.apache.jena.rdf.model.{Literal, Model, Resource}

import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 17:24
 * @Modified By:
 */
trait SerializableFunction[-T, +R] extends (T => R) with Serializable

trait RemoteDataFrame extends Serializable {
  val source: DataAccessRequest
  val ops: List[DFOperation]

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
  def max(column: String): RemoteDataFrameImpl = {
    RemoteDataFrameImpl(remoteDataFrameImpl.source, remoteDataFrameImpl.ops :+ MaxOp(column), remoteDataFrameImpl.client)
  }
  //可自定义聚合函数
}

case class RemoteDataFrameImpl(source: DataAccessRequest, ops: List[DFOperation], client: ArrowFlightClient = null) extends RemoteDataFrame with Logging {

  override def filter(f: Row => Boolean): RemoteDataFrame = {
    copy(ops = ops :+ FilterOp(new SerializableFunction[Row, Boolean] {
      override def apply(v1: Row): Boolean = f(v1)
    }))
  }

  override def select(columns: String*): RemoteDataFrame = {
    copy(ops = ops :+ SelectOp(columns))
  }

  override def limit(n: Int): RemoteDataFrame = {
    copy(ops = ops :+ LimitOp(n))
  }

  override def foreach(f: Row => Unit): Unit = records.foreach(f)

  override def collect(): List[Row] = records.toList

  override def map(f: Row => Row): RemoteDataFrame = {
    copy(ops = ops :+ MapOp(new SerializableFunction[Row, Row] {
      override def apply(v1: Row): Row = f(v1)
    }))
  }

  override def reduce(f: ((Row, Row)) => Row): RemoteDataFrame = {
    copy(ops = ops :+ ReduceOp(new SerializableFunction[(Row, Row), Row] {
      override def apply(v1: (Row, Row)): Row = f(v1)
    }))
  }

  def groupBy(column: String): GroupedDataFrame = {
    copy(ops = ops :+ GroupByOp(column))
    GroupedDataFrame(this)
  }

  private def records(): Iterator[Row] = client.getRows(source, ops)

  override def getSchema: String = client.getSchema(source.dataFrame)

  override def getSchemaURI: String = client.getSchemaURI(source.dataFrame)
}



