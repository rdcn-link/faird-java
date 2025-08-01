package link.rdcn.client.dag

import link.rdcn.struct.DataFrame

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/12 21:07
 * @Modified By:
 */
trait FlowNode

trait Transformer11 extends FlowNode with Serializable {
  def transform(dataFrame: DataFrame): DataFrame
}

case class JavaCodeNode(
                   clazz: Map[String,Array[Byte]]
                   ) extends FlowNode

case class RepositoryNode(
                    functionId: String,
                  ) extends FlowNode


//只为DAG执行提供dataFrameName
case class SourceNode (dataFrameName: String) extends FlowNode

object FlowNode {
  def source(dataFrameName: String): SourceNode = {
    SourceNode(dataFrameName)
  }

  def fromJavaClass(clazz: Map[String,Array[Byte]]): JavaCodeNode = {
    JavaCodeNode(clazz)
  }

  def fromJavaClass(func: DataFrame => DataFrame): Transformer11 = {
    (dataFrame: DataFrame) => func(dataFrame)
  }

  def stocked(functionId: String): RepositoryNode = {
    RepositoryNode(functionId)
  }

}