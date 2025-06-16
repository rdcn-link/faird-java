package org.grapheco.client

import org.apache.spark.sql.Row
import org.grapheco.server.SerializableFunction

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 14:42
 * @Modified By:
 */
sealed trait DFOperation extends Serializable

case class MapOp(f: SerializableFunction[Row, Row]) extends DFOperation
case class FilterOp(f: SerializableFunction[Row, Boolean]) extends DFOperation
case class SelectOp(cols: Seq[String]) extends DFOperation
case class LimitOp(n: Int) extends DFOperation

case class ReduceOp(f: SerializableFunction[(Row, Row), Row]) extends DFOperation
case class MaxOp(column: String) extends DFOperation
case class GroupByOp(column: String) extends DFOperation