package org.grapheco.client

import org.apache.spark.sql.Row
import org.grapheco.server.SerializableFunction

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 14:42
 * @Modified By:
 */
sealed trait DFOperation extends Serializable {
  def transform(input: Iterator[Row]): Iterator[Row]
}

case class MapOp(f: SerializableFunction[Row, Row]) extends DFOperation {
  override def transform(input: Iterator[Row]): Iterator[Row] = input.map(f(_))
}
case class FilterOp(f: SerializableFunction[Row, Boolean]) extends DFOperation {
  override def transform(input: Iterator[Row]): Iterator[Row] = input.filter(f(_))
}
case class SelectOp(cols: Seq[String]) extends DFOperation {
  override def transform(input: Iterator[Row]): Iterator[Row] = ???
}
case class LimitOp(n: Int) extends DFOperation {
  override def transform(input: Iterator[Row]): Iterator[Row] = input.take(n)
}

case class ReduceOp(f: SerializableFunction[(Row, Row), Row]) extends DFOperation {
  override def transform(input: Iterator[Row]): Iterator[Row] = ???
}
case class MaxOp(column: String) extends DFOperation {
  override def transform(input: Iterator[Row]): Iterator[Row] = ???
}
case class GroupByOp(column: String) extends DFOperation {
  override def transform(input: Iterator[Row]): Iterator[Row] = ???
}