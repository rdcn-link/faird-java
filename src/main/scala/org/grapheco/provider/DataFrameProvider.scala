package org.grapheco.provider

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 17:46
 * @Modified By:
 */
import org.apache.arrow.vector.{IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.jena.rdf.model.Model
import org.grapheco.Logging

import java.io.{File, FileInputStream}
import scala.collection.Seq
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
  // 参数是目录、 目录/*.csv, Iterator[ArrowRecordBatch] 5mb batch size

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

  private def createFileChunkBatch(arrowRoot: VectorSchemaRoot): ArrowRecordBatch = {
    arrowRoot.allocateNew()
    var index = 0
    val path = s"C:\\Users\\Yomi\\Downloads\\数据\\1.csv"
    val idVec = arrowRoot.getVector("id").asInstanceOf[IntVector]
    val nameVec = arrowRoot.getVector("name").asInstanceOf[VarCharVector]
    val indexVec = arrowRoot.getVector("chunkIndex").asInstanceOf[IntVector]
    val contentVec = arrowRoot.getVector("bin").asInstanceOf[VarBinaryVector]
    for (i <- 0 to 5 - 1) {
      val file = new File(path)
      val fileName = file.getName
      var chunkCount = 0
      readFileInChunks(file).foreach(bytes => {
        idVec.setSafe(index, i)
        nameVec.setSafe(index, fileName.getBytes())
        indexVec.setSafe(index,chunkCount)
        contentVec.setSafe(index, bytes)
        index += 1
        chunkCount += 1
      })
    }
    arrowRoot.setRowCount(index)
    val unloader = new VectorUnloader(arrowRoot)
    unloader.getRecordBatch
  }

  private def readFileInChunks(file: File, chunkSize: Int = 5 * 1024 * 1024): Iterator[Array[Byte]] = {

    val inputStream = new FileInputStream(file)

    new Iterator[Array[Byte]] {
      override def hasNext: Boolean = inputStream.available() > 0

      override def next(): Array[Byte] = {
        val bufferSize = Math.min(chunkSize, inputStream.available())
        val buffer = new Array[Byte](bufferSize)
        val bytesRead = inputStream.read(buffer)
        if (bytesRead == -1) {
          inputStream.close()
          Iterator.empty.next()
        } else if (bytesRead < buffer.length) {
          inputStream.close()
          buffer.take(bytesRead)
        } else {
          buffer
        }
      }
    }

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

class SimpleDataFrameSourceFactory extends DataFrameSourceFactory with Logging{
  override def createFileListDataFrameSource(path: String, pattern: String): DataFrameSource = {
    log.info(s"create dataFrame from $path/$pattern")
    FileDataFrameSource(s"$path/$pattern")
  }
}

