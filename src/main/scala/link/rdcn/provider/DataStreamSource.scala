package link.rdcn.provider

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 17:46
 * @Modified By:
 */

import link.rdcn.struct.ValueType.BinaryType
import link.rdcn.struct.{DataFrame, Row, StructType}
import link.rdcn.util.DataUtils
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch

import java.io.File
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.{Seq, mutable}

trait DataStreamSource {
  def process(stream: Iterator[Row], root: VectorSchemaRoot, batchSize: Int): Iterator[ArrowRecordBatch]
  def createDataFrame(): DataFrame
}

case class ArrowFlightDataStreamSource(iter: Iterator[Row], schema: StructType) extends DataStreamSource {

  override def process(stream: Iterator[Row], root: VectorSchemaRoot, batchSize: Int): Iterator[ArrowRecordBatch] = {
    val streamSplitChunk: Iterator[Row] = if(schema.contains(BinaryType)){
      stream.flatMap(row => {
        val file = row.getAs[File](7).get
        DataUtils.readFileInChunks(file).map(bytes => {
          (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6), bytes)
        })
      }).map(Row.fromTuple(_))
    }else stream
    streamSplitChunk.grouped(batchSize).map(rows => createDummyBatch(root, rows))
  }

  override def createDataFrame(): DataFrame = DataFrame(schema, iter)

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
        j += 1
      })
      i += 1
    })
    arrowRoot.setRowCount(rows.length)
    val unloader = new VectorUnloader(arrowRoot)
    unloader.getRecordBatch
  }
}



