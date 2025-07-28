package link.rdcn.client.dag

import scala.annotation.varargs

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
case class Flow(
                           nodes: Map[String, FlowNode],
                           edges: Map[String, Seq[String]]
                         ){

  def getExecutionPaths(): Seq[Seq[FlowNode]] = {
    if(edges.isEmpty){
      nodes.map(node => Seq(node._2)).toSeq
    }else{
      val keyPaths = extractAllPaths(edges)
      val nodePaths:Seq[Seq[FlowNode]] = keyPaths.map(_.map(key => nodes.get(key).getOrElse(throw new IllegalArgumentException(
        s"Invalid DAG: root node '$key' is not defined in the node map."
      ))))
      nodePaths
    }
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
    if(rootNodes.isEmpty) throw new IllegalArgumentException("Invalid DAG: no root nodes found, graph might contain cycles or be empty")
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

    def dfs(path: Seq[String], current: String, visitedInPath: Set[String]): Seq[Seq[String]] = {
      if (visitedInPath.contains(current)) {
        throw new IllegalArgumentException(s"Cycle detected: node '$current' is revisited in path ${path.mkString(" -> ")}")
      }

      edges.get(current) match {
        case Some(children) =>
          children.flatMap(child => dfs(path :+ child, child, visitedInPath + current))
        case None =>
          Seq(path)
      }
    }

    rootNodes.toSeq.flatMap(root => dfs(Seq(root), root, Set.empty))
  }
}
object Flow{
  @varargs
  def pipe(head: FlowNode, tail: FlowNode*): Flow = {
    val nodes = head +: tail
    if (nodes.isEmpty) {
      throw new IllegalArgumentException("one node at least")
    }
    val pairs = nodes.zipWithIndex.map {
      case (node, index) =>
        (index.toString, node)
    }.toMap
    val keysSeq =  pairs.keys.toSeq
    val edges = keysSeq.zip(keysSeq.tail).map { case (currentElement, nextElement) =>
      currentElement -> Seq(nextElement)}.toMap
    Flow(pairs, edges)
  }
}

