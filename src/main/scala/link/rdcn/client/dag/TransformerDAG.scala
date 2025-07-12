package link.rdcn.client.dag

import link.rdcn.dftree.{Operation, SourceOp}

/**
 * @Author renhao
 * @Description:
 * TransfomerDAG(
 * Nodes(
 *      "A" -> SourceOp("/abcd"),
 *      "B" -> Functiion.STOCKED("cn.piflow.Transfomer1", params),
 *      "C" -> Function.PYTHON_Code(code),
 *      "D" -> Function.JAVA(bytes2)
 *     ),
 * Edges(
 *     "A" -> Seq("B", "C"),
 *     "B" -> "D"
 *     )
 * )
 * @Data 2025/7/12 21:30
 * @Modified By:
 */
case class TransformerDAG(
                           nodes: Map[String, DAGNode],
                           edges: Map[String, Seq[String]]
                         ){


  def getExecutionPaths(): Seq[Seq[DAGNode]] = {
    val keyPaths = extractAllPaths(edges)
    val nodePaths:Seq[Seq[DAGNode]] = keyPaths.map(_.map(key => nodes.get(key).getOrElse(throw new IllegalArgumentException(
      s"Invalid DAG: root node '$key' is not defined in the node map."
    ))))
    nodePaths
  }
  /**
   * Edges(
   * "A" -> Seq("B", "C"),        A->B->D
   * "B" -> "D"               =>  A->C
   * )
   * )
   * */
  private def extractAllPaths(edges: Map[String, Seq[String]]): Seq[Seq[String]] = {
    val allTargets = edges.values.flatten.toSet
    val allSources = edges.keySet
    val rootNodes = allSources.diff(allTargets) // 没有被其他节点指向的起始节点
    rootNodes.foreach(rootKey => nodes.get(rootKey) match {
      case Some(s: SourceNode) => // 合法，继续
      case Some(other) =>
        throw new IllegalArgumentException(
          s"Invalid DAG: root node '$other' is not of type SourceOp, but ${other.getClass.getSimpleName}."
        )
      case None =>
        throw new IllegalArgumentException(
          s"Invalid DAG: root node '$rootKey' is not defined in the node map."
        )
    })

    def dfs(path: Seq[String], current: String): Seq[Seq[String]] = {
      edges.get(current) match {
        case Some(children) =>
          children.flatMap(child => dfs(path :+ child, child))
        case None =>
          Seq(path) // 当前节点没有 children，是终点
      }
    }

    rootNodes.toSeq.flatMap(root => dfs(Seq(root), root))
  }
}

