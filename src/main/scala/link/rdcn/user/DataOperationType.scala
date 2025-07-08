package link.rdcn.user

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/8 18:14
 * @Modified By:
 */
object DataOperationType extends Enumeration {
  type DataOperationType = Value

  val Map: Value    = Value("Map")
  val Filter: Value = Value("Filter")
  val Select: Value = Value("Select")
  val Reduce: Value = Value("Reduce")
  val Join: Value   = Value("Join")
  val GroupBy: Value = Value("GroupBy")
  val Sort: Value    = Value("Sort")
}
