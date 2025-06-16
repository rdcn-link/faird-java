package org.grapheco.server

import org.apache.spark.sql.Row
import org.grapheco.client.{Blob, DFOperation, FilterOp, GroupByOp, LimitOp, MapOp, MaxOp, ReduceOp, SelectOp}

import java.nio.charset.StandardCharsets

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 17:24
 * @Modified By:
 */
trait SerializableFunction[-T, +R] extends (T => R) with Serializable

trait RemoteDataFrame extends Serializable {
  def map(f: Row => Row): RemoteDataFrame
  def filter(f: Row => Boolean): RemoteDataFrame
  def select(columns: String*): RemoteDataFrame
  def limit(n: Int): RemoteDataFrame

  def reduce(f: ((Row, Row)) => Row): RemoteDataFrame

  def foreach(f: Row => Unit): Unit // 远程调用 + 拉取结果
  def collect(): List[Row]
}

case class GroupedDataFrame(remoteDataFrameImpl: RemoteDataFrameImpl){
  def max(column: String): RemoteDataFrameImpl = {
    RemoteDataFrameImpl(remoteDataFrameImpl.source, remoteDataFrameImpl.ops :+ MaxOp(column), remoteDataFrameImpl.remoteExecutor)
  }
  //可自定义聚合函数
}

case class RemoteDataFrameImpl(source: String, ops: List[DFOperation],remoteExecutor: RemoteExecutor = null) extends RemoteDataFrame {
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

  def foreach(f: Row => Unit): Unit = remoteExecutor.execute(source, ops).foreach(f)

  override def collect(): List[Row] = remoteExecutor.execute(source, ops).toList

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
}



