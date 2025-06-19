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
import org.grapheco.client.{CSVSource, DFOperation, StructuredSource}
import org.grapheco.server.RemoteDataFrame
import org.grapheco.util.DataUtils

import java.io.{File, FileInputStream}
import scala.collection.Seq
import scala.io.Source
import scala.jdk.CollectionConverters.asScalaBufferConverter

trait DataFrameSource {
  def getArrowRecordBatch(root: VectorSchemaRoot): Iterator[ArrowRecordBatch]
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

  private def createDummyBatch(arrowRoot: VectorSchemaRoot, rows: Seq[Row]): ArrowRecordBatch = {
    arrowRoot.allocateNew()
    val fieldVectors = arrowRoot.getFieldVectors.asScala
    for (i <- rows.indices) {
      val row = rows(i)
      for (j <- 0 until row.length) {
        val value = row.get(j)
        val vec = fieldVectors(j)
        // 支持基本类型处理（可扩展）
        value match {
          case v: Int => vec.asInstanceOf[IntVector].setSafe(i, v)
          case v: Long => vec.asInstanceOf[BigIntVector].setSafe(i, v)
          case v: Double => vec.asInstanceOf[Float8Vector].setSafe(i, v)
          case v: Float => vec.asInstanceOf[Float4Vector].setSafe(i, v)
          case v: String =>
            val bytes = v.getBytes("UTF-8")
            vec.asInstanceOf[VarBinaryVector].setSafe(i, bytes, 0, bytes.length)
          case v: Boolean => vec.asInstanceOf[BitVector].setSafe(i, if (v) 1 else 0)
          case v: Array[Byte] => vec.asInstanceOf[VarBinaryVector].setSafe(i, v)
          case null => vec.setNull(i)
          case _ => throw new UnsupportedOperationException("Type not supported")
        }
      }
    }
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
    val stream: Iterator[Seq[Row]] = remoteDataFrame.source.inputSource match {
      case CSVSource(delimiter) => DataUtils.groupedLines(s"$dataSet/$dataFrameName", 1000).map(seq => {
        seq.map(ss => Row(ss.split(delimiter): _*))
      })
      case StructuredSource() => DataUtils.groupedLines(s"$dataSet/$dataFrameName", 1000).map(seq => {
        seq.map(ss => Row(ss))
      })
    }
    val result = applyOperations(stream.flatten, remoteDataFrame.ops)
    DataFrameSourceImpl(result.grouped(batchSize))
  }

  private def applyOperations(stream: Iterator[Row], ops: List[DFOperation]): Iterator[Row] = {
    ops.foldLeft(stream) { (acc, op) => op.transform(acc) }
  }

}


