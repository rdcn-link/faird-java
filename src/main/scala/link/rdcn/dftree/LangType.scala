package link.rdcn.dftree

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/2 09:22
 * @Modified By:
 */

sealed trait LangType {
  def name: String
}
object LangType {
  case object JAVA_BIN extends LangType { val name = "JAVA_BIN" }
  case object PYTHON extends LangType { val name = "PYTHON" }
  case object TYPESCRIPT extends LangType { val name = "TYPESCRIPT" }

  val all: List[LangType] = List(JAVA_BIN, PYTHON, TYPESCRIPT)
  def fromName(s: String): LangType = all.find(_.name == s).getOrElse(throw new Exception(s"$s This programming language is not supported"))
}

