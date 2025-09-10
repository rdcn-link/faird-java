package link.rdcn.struct

trait ExecutionResult {
  def single(): DataFrame

  def get(name: String): DataFrame

  def map(): Map[String, DataFrame]
}
