package link.rdcn.client.dag

import link.rdcn.client.RemoteDataFrame
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

//只为DAG执行提供dataFrameName
case class SourceNode (dataFrameName: String) extends DAGNode