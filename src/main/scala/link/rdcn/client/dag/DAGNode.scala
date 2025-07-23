package link.rdcn.client.dag

import link.rdcn.struct.Row

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/12 21:07
 * @Modified By:
 */
trait DAGNode

trait UDFFunction extends DAGNode with Serializable {
  def transform(iter: Iterator[Row]): Iterator[Row]
}
case class PythonWhlFunctionNode(
                            functionId: String,
                            functionName: String,
                            whlPath: String
                            ) extends DAGNode
case class JavaCodeNode(
                   javaCode: String,
                   className: String
                   ) extends DAGNode

case class PythonCodeNode(
                     code: String
                     ) extends DAGNode



//只为DAG执行提供dataFrameName
case class SourceNode (dataFrameName: String) extends DAGNode