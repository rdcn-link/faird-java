package link.rdcn.struct

import link.rdcn.util.ClosableIterator

import scala.annotation.varargs

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/23 16:23
 * @Modified By:
 */
trait DataFrame {
  val schema: StructType

  def mapIterator[T](f: ClosableIterator[Row] => T): T

  def map(f: Row => Row): DataFrame

  def filter(f: Row => Boolean): DataFrame

  @varargs
  def select(columns: String*): DataFrame

  def limit(n: Int): DataFrame

  def reduce(f: ((Row, Row)) => Row): DataFrame

  def foreach(f: Row => Unit): Unit

  def collect(): List[Row]
}