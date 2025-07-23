package link.rdcn.server

import link.rdcn.struct.{DataFrameStream, Row}
import link.rdcn.struct.ValueType.BinaryType
import link.rdcn.util.DataUtils
import org.apache.arrow.vector.{BigIntVector, BitVector, Float4Vector, Float8Vector, IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch

import java.io.File
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/8 14:22
 * @Modified By:
 */
case class ArrowFlightStreamWriter(dataFrame: DataFrameStream) {

  val schema = dataFrame.schema
  val stream = dataFrame.stream

  def process(root: VectorSchemaRoot, batchSize: Int): Iterator[ArrowRecordBatch] = {
    val streamSplitChunk: Iterator[Row] = if(schema.contains(BinaryType)){
      stream.flatMap(row => {
        val file = row.getAs[File](6).get
        DataUtils.readFileInChunks(file).map(bytes => {
          (row._1, row._2, row._3, row._4, row._5, row._6, bytes)
        })
      }).map(Row.fromTuple(_))
    }else stream
    streamSplitChunk.grouped(batchSize).map(rows => createDummyBatch(root, rows))
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
        j += 1
      })
      i += 1
    })
    arrowRoot.setRowCount(rows.length)
    val unloader = new VectorUnloader(arrowRoot)
    unloader.getRecordBatch
  }
}
