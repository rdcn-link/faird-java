package org.grapheco.provider

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 17:46
 * @Modified By:
 */
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.Row
import org.grapheco.Logging
import org.grapheco.client.{CSVSource, DFOperation, DirectorySource, StructuredSource}
import org.grapheco.server.RemoteDataFrame
import org.grapheco.util.DataUtils

import java.io.{File, FileInputStream}
import scala.collection.Seq
import scala.io.Source
import scala.jdk.CollectionConverters.asScalaBufferConverter

trait DataFrameSource {
  def getArrowRecordBatch(root: VectorSchemaRoot): Iterator[ArrowRecordBatch]
  def getFilesArrowRecordBatch(root: VectorSchemaRoot, chunkSize: Int  = 5 * 1024 * 1024, batchSize: Int = 10): Iterator[ArrowRecordBatch]
}

trait DataFrameSourceFactory {
  def createFileListDataFrameSource(remoteDataFrame: RemoteDataFrame): DataFrameSource
}

case class DataFrameSourceImpl(iter: Iterator[Seq[Row]]) extends DataFrameSource {
  val batchSize = 1000

  //处理结构化数据,row -> 一行数据
  override def getArrowRecordBatch(root: VectorSchemaRoot): Iterator[ArrowRecordBatch] = {
    iter.map(rows => createDummyBatch(root, rows))
  }

  override def getFilesArrowRecordBatch(root: VectorSchemaRoot, chunkSize: Int  = 5 * 1024 * 1024, batchSize: Int = 10): Iterator[ArrowRecordBatch] = {
    // 将文件转换为迭代器：(文件名, 5MB chunk数据)
    val files = DataUtils.listFiles("C:\\Users\\Yomi\\Downloads\\数据\\cram")
    val chunkIterators = files.iterator.zipWithIndex.map { case (file,index) =>
      (index, file.getName, DataUtils.readFileInChunks(file, chunkSize))
    }
    val allChunks = chunkIterators.flatMap { case (index, filename, chunks) =>
      chunks.map(chunk => (index, filename, chunk))
    }

    DataUtils.createFileChunkBatch(allChunks,root)
  }


  // 列出目录下所有文件


  def createFileBatch(arrowRoot: VectorSchemaRoot, seq: Seq[Row]): ArrowRecordBatch = {
    arrowRoot.allocateNew()

    val vec = arrowRoot.getVector("name").asInstanceOf[VarBinaryVector]

    var i = 0
    seq.foreach(ss => {
      vec.setSafe(i, ss.get(0).toString.getBytes("UTF-8"))
      i += 1
    })

    arrowRoot.setRowCount(i)
    val unloader = new VectorUnloader(arrowRoot)
    unloader.getRecordBatch
  }

  private def createDummyBatch(arrowRoot: VectorSchemaRoot, rows: Seq[Row]): ArrowRecordBatch = {
    arrowRoot.allocateNew()
    val fieldVectors = arrowRoot.getFieldVectors.asScala
    var i = 0
    rows.foreach(row => {
      var j = 0
      fieldVectors.foreach(vec => {
        val value = row.get(j)
        value match {
          case v: Int => vec.asInstanceOf[IntVector].setSafe(i, v)
          case v: Long => vec.asInstanceOf[BigIntVector].setSafe(i, v)
          case v: Double => vec.asInstanceOf[Float8Vector].setSafe(i, v)
          case v: Float => vec.asInstanceOf[Float4Vector].setSafe(i, v)
          case v: String =>
            val bytes = v.getBytes("UTF-8")
            vec.asInstanceOf[VarCharVector].setSafe(i, bytes)
          case v: Boolean => vec.asInstanceOf[BitVector].setSafe(i, if (v) 1 else 0)
          case v: Array[Byte] => vec.asInstanceOf[VarBinaryVector].setSafe(i, v)
          case null => vec.setNull(i)
          case _ => throw new UnsupportedOperationException("Type not supported")
        }
        i += 1
        j += 1
      })
//      for (j <- 0 until row.length) {
//        val value = row.get(j)
//        val vec = fieldVectors(j)
//        // 支持基本类型处理（可扩展）
//        value match {
//          case v: Int => vec.asInstanceOf[IntVector].setSafe(i, v)
//          case v: Long => vec.asInstanceOf[BigIntVector].setSafe(i, v)
//          case v: Double => vec.asInstanceOf[Float8Vector].setSafe(i, v)
//          case v: Float => vec.asInstanceOf[Float4Vector].setSafe(i, v)
//          case v: String =>
//            val bytes = v.getBytes("UTF-8")
//            vec.asInstanceOf[VarBinaryVector].setSafe(i, bytes, 0, bytes.length)
//          case v: Boolean => vec.asInstanceOf[BitVector].setSafe(i, if (v) 1 else 0)
//          case v: Array[Byte] => vec.asInstanceOf[VarBinaryVector].setSafe(i, v)
//          case null => vec.setNull(i)
//          case _ => throw new UnsupportedOperationException("Type not supported")
//        }
//        i += 1
//      }
    })
    arrowRoot.setRowCount(rows.length)
    val unloader = new VectorUnloader(arrowRoot)
    unloader.getRecordBatch
  }
}

class DataFrameSourceFactoryImpl extends DataFrameSourceFactory with Logging{
  val batchSize = 1000
  override def createFileListDataFrameSource(remoteDataFrame: RemoteDataFrame): DataFrameSource = {
//    /Users/renhao/Downloads
    val dataSet = remoteDataFrame.source.datasetId
    val dataFrameName = remoteDataFrame.source.dataFrames
    log.info(s"create dataFrame from $dataSet/$dataFrameName")
    //根据dataSet dataFrameName拉取数据，目前这里dataSet代表路径 dataFrameName代表文件名称
    val stream: Iterator[Row] = remoteDataFrame.source.inputSource match {
      case CSVSource(delimiter) => DataUtils.getFileLines(s"$dataSet/$dataFrameName").map(line => {
        Row(line.split(delimiter): _*)
      })
      case StructuredSource() => DataUtils.getFileLines(s"$dataSet/$dataFrameName").map(line => {
        Row(line)
      })
    }
    val result = applyOperations(stream, remoteDataFrame.ops)
    DataFrameSourceImpl(result.grouped(batchSize))
  }

  private def applyOperations(stream: Iterator[Row], ops: List[DFOperation]): Iterator[Row] = {
    ops.foldLeft(stream) { (acc, op) => op.transform(acc) }
  }

}


