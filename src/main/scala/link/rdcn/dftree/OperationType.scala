package link.rdcn.dftree

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/2 09:21
 * @Modified By:
 */
sealed trait OperationType {
  def name: String
}
object OperationType {
  case object Map extends OperationType { val name = "Map" }
  case object Filter extends OperationType { val name = "Filter" }
  case object Source extends OperationType { val name = "Source" }
  case object Limit extends OperationType { val name = "Limit" }
  case object Select extends OperationType {val name ="Select"}
  case object Reduce extends OperationType {val name = "Reduce" }
  // ...
  val all: List[OperationType] = List(Map, Filter, Source, Limit, Select, Reduce)
  def fromName(s: String): OperationType = all.find(_.name == s).getOrElse(throw new Exception(s"$s This operation type is not supported"))
}

