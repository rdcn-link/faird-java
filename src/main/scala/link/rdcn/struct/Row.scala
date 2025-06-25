package link.rdcn.struct

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 15:05
 * @Modified By:
 */

class Row (val values: Seq[Any]) {
  /** 行长度（列数） */
  def length: Int = values.length

  /** 按索引获取值，越界则抛 IndexOutOfBoundsException */
  def get(index: Int): Any = values(index)

  /** 按索引获取 Option，如果越界返回 None */
  def getOpt(index: Int): Option[Any] =
    if (index >= 0 && index < values.length) Some(values(index)) else None

  /** 将行转为 Seq[Any] */
  def toSeq: Seq[Any] = values

  /** 转成 Array[Any] */
  def toArray: Array[Any] = values.toArray

  /** 获取迭代器 */
  def iterator: Iterator[Any] = values.iterator

  override def toString: String = {
    val elems = values.map {
      case null    => "null"
      case arr: Array[_] => arr.mkString("Array(", ", ", ")")
      case other   => other.toString
    }
    s"Row(${elems.mkString(", ")})"
  }

  /** getAs[T]: 尝试强转，成功返回 Some，否则 None（也会对 null 返回 None） */
  def getAs[T](index: Int): Option[T] = getOpt(index) match {
    case Some(v) if v != null =>
      try Some(v.asInstanceOf[T])
      catch { case _: ClassCastException => None }
    case _ => None
  }
}

object Row {
  /** Varargs 方式构造：Row(1, "Li", true) 等 */
  def apply(values: Any*): Row = new Row(values)

  /** 从 Seq[Any] 构造 */
  def fromSeq(seq: Seq[Any]): Row = new Row(seq)

  /** 从 Array[Any] 构造 */
  def fromArray(arr: Array[Any]): Row = new Row(arr.toSeq)

  def fromTuple(tuple: Product): Row = fromSeq(tuple.productIterator.toSeq)

  /** 空行 */
  val empty: Row = new Row(Seq.empty)

  /** Helper: 与索引和值一起迭代 */
  def zipWithIndex(row: Row): Seq[(Int, Any)] =
    row.values.zipWithIndex.map { case (v, idx) => (idx, v) }
}

