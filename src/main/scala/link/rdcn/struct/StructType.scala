package link.rdcn.struct
import link.rdcn.struct.ValueType._

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 14:55
 * @Modified By:
 */
case class Column(name: String, colType: ValueType,  nullable: Boolean = true)

case class StructType(columns: Seq[Column]) {

  private val nameToIndex: Map[String, Int] = {
    val pairs = columns.zipWithIndex.map { case (col, idx) => col.name -> idx }
    val dupNames = pairs.groupBy(_._1).collect { case (n, xs) if xs.size > 1 => n }
    require(dupNames.isEmpty, s"StructType 构造失败：存在重复列名 ${dupNames.mkString(",")}")
    pairs.toMap
  }

  def isEmpty(): Boolean = columns.isEmpty

  /** 根据列名获取类型（可选） */
  def getType(columnName: String): Option[ValueType] =
    columns.find(_.name == columnName).map(_.colType)

  /** 判断某列是否存在 */
  def contains(columnName: String): Boolean =
    columns.exists(_.name == columnName)

  def contains(colType: ValueType): Boolean =
    columns.exists(_.colType == colType)

  /** 返回所有列名 */
  def columnNames: Seq[String] = columns.map(_.name)

  /** 根据列名获取索引，第几列，从 0 开始；找不到返回 None */
  def indexOf(columnName: String): Option[Int] =
    nameToIndex.get(columnName)

  /** 根据索引获取列定义；越界时抛异常 */
  def columnAt(index: Int): Column =
    if (index >= 0 && index < columns.length) columns(index)
    else throw new IndexOutOfBoundsException(s"columnAt: 索引 $index 越界，列数 ${columns.length}")

  def add(name: String, colType: ValueType, nullable: Boolean = true): StructType =
    this.copy(columns = columns :+ Column(name, colType, nullable))

  override def toString: String =
      columns.map(c => s"${c.name}: ${c.colType}").mkString("schema(", ", ", ")")
}

object StructType {
  /** varargs 构造：StructType(Column("id", IntType), ...) */
  def fromColumns(cols: Column*): StructType = new StructType(cols)

  /** 避免与 case class 自动生成 apply 冲突 */
  def fromSeq(cols: Seq[Column]): StructType = new StructType(cols)

  def fromNamesAndTypes(pairs: (String, ValueType)*): StructType =
    new StructType(pairs.map { case (n, t) => Column(n, t) })

  def fromNamesAsAny(names: Seq[String]): StructType =
    new StructType(names.map(n => Column(n, ValueType.StringType)))

  val empty: StructType = new StructType(Seq.empty)

  def binaryStructType: StructType = {
    StructType.empty.add("name", StringType).add("size", LongType).add("type", StringType)
      .add("creationTime", LongType).add("lastModifiedTime", LongType).add("lastAccessTime", LongType)
      .add("File",BinaryType)
  }
}
