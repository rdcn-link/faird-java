package org.grapheco.provider

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 17:46
 * @Modified By:
 */
import org.apache.arrow.vector.{VarBinaryVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.jena.rdf.model.Model

import scala.collection.immutable.Seq
import scala.io.Source

trait DataFrameProvider {
  def checkPermission(dataFrameName: String, userId: String, operation: String): Boolean
  def listDataSetNames(): List[String]
  def getDataSetMetaData(dataSetName: String, rdfModel: Model): Unit
  def listDataFrameNames(dataSetName: String): List[String]
  def getDataFrameSource(dataFrameName: String, factory: DataFrameSourceFactory): DataFrameSource
}

trait DataFrameSource {
  def sourceUri: String
  def getArrowRecordBatch(root: VectorSchemaRoot): Iterator[ArrowRecordBatch]
}

trait DataFrameSourceFactory {
  def createFileListDataFrameSource(path: String, pattern: String): DataFrameSource
}

case class FileDataFrameSource(sourceUri: String) extends DataFrameSource {
  val batchSize = 1000

  //处理结构化数据,row -> 一行数据
  override def getArrowRecordBatch(root: VectorSchemaRoot): Iterator[ArrowRecordBatch] = {
    groupedLines(sourceUri, batchSize).map(lines => createFileBatch(root, lines))
  }
  //TODO 处理非结构化数据，row -> 对应一个文件



  //结构化文件分批传输
  private def createFileBatch(arrowRoot: VectorSchemaRoot, seq: Seq[String]): ArrowRecordBatch = {
    arrowRoot.allocateNew()
    //TODO 根据arrowRoot获取schema 返回Seq[Seq[String]]
    val vec = arrowRoot.getVector("name").asInstanceOf[VarBinaryVector]

    var i = 0
    seq.foreach(ss => {
      vec.setSafe(i, ss.getBytes("UTF-8"))
      i += 1
    })

    arrowRoot.setRowCount(i)
    val unloader = new VectorUnloader(arrowRoot)
    unloader.getRecordBatch
  }

  private def groupedLines(filePath: String, batchSize: Int): Iterator[Seq[String]] = {
    val source = Source.fromFile(filePath)
    val iter = source.getLines()

    // 创建包装的 Iterator 来确保文件在迭代结束后被关闭
    new Iterator[Seq[String]] {
      override def hasNext: Boolean = {
        val hn = iter.hasNext
        if (!hn) source.close()
        hn
      }

      override def next(): Seq[String] = iter.take(batchSize).toSeq
    }
  }
}

class SimpleDataFrameSourceFactory extends DataFrameSourceFactory {
  override def createFileListDataFrameSource(path: String, pattern: String): DataFrameSource = {
    FileDataFrameSource(s"file://$path/$pattern")
  }
}

