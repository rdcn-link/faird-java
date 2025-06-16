package org.grapheco.client

import org.apache.arrow.flight.{AsyncPutListener, FlightClient, FlightDescriptor, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.Row
import org.grapheco.SimpleSerializer

import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{asScalaBufferConverter, seqAsJavaListConverter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 14:45
 * @Modified By:
 */
class FlightDataClient(url: String, port:Int) {

  val location = Location.forGrpcInsecure(url, port)
  val allocator: BufferAllocator = new RootAllocator()
  val flightClient = FlightClient.builder(allocator, location).build()

  def getRows(source: String, ops: List[DFOperation]): Iterator[Row] = {
    //上传参数
    val paramFields: Seq[Field] = List(
      new Field("source", FieldType.nullable(new ArrowType.Utf8()), null),
      new Field("DFOperation", FieldType.nullable(new ArrowType.Binary()), null)
    )
    val schema = new Schema(paramFields.asJava)
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val varCharVector = vectorSchemaRoot.getVector("source").asInstanceOf[VarCharVector]
    val DFOperationVector = vectorSchemaRoot.getVector("DFOperation").asInstanceOf[VarBinaryVector]
    varCharVector.allocateNew(1)
    varCharVector.set(0, source.getBytes)
    if(ops.length == 0){
      DFOperationVector.allocateNew(1)
      vectorSchemaRoot.setRowCount(1)
    }else{
      DFOperationVector.allocateNew(ops.length)
      for(i <- 0 to ops.length -1){
        DFOperationVector.set(i, SimpleSerializer.serialize(ops(i)))
      }
      vectorSchemaRoot.setRowCount(ops.length)
    }

    val requestSchemaId = UUID.randomUUID().toString
    val listener = flightClient.startPut(FlightDescriptor.path(requestSchemaId), vectorSchemaRoot, new AsyncPutListener())
    listener.putNext()
    listener.completed()
    listener.getResult()
    //获取数据
    val flightInfo = flightClient.getInfo(FlightDescriptor.path(requestSchemaId))
    //flightInfo 中可以获取schema
    println(s"Client (Get Metadata): $flightInfo")
    val flightStream = flightClient.getStream(flightInfo.getEndpoints.get(0).getTicket)
    new Iterator[Seq[Row]] {
      override def hasNext: Boolean = flightStream.next()


      override def next(): Seq[Row] = {
        val vectorSchemaRootReceived = flightStream.getRoot
        val rowCount = vectorSchemaRootReceived.getRowCount
        val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
        Seq.range(0, rowCount).map(index => {
          val rowMap = fieldVectors.map(vec => {
            if(vec.isNull(index)) (vec.getName, null)
            else vec match {
              case v: org.apache.arrow.vector.IntVector     => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.VarCharVector => (vec.getName, new String(v.get(index)))
              case v: org.apache.arrow.vector.Float8Vector  => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.BitVector     => (vec.getName, v.get(index) == 1)
              case v: org.apache.arrow.vector.VarBinaryVector =>(vec.getName, v.get(index))
              case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getClass}")
            }
          }).toMap
          Row(rowMap.toSeq.map(x => x._2): _*)
        })
      }
    }.flatMap(rows => rows)
  }

  def getBlobs(source: String, ops: List[DFOperation]): Iterator[Row] = {
    // 使用映射追踪文件ID对应的所有分块
    val blobMap = mutable.Map[String, ArrayBuffer[Array[Byte]]]()
    val idVectorName = "id"  // 假设ID列名为"id"
    val chunkVectorName = "chunk"  // 假设分块列名为"chunk"
    var processedRows = 0
    // 处理每一行数据
    for (row <- getRows(source, ops)) {
      processedRows += 1

      // 从Row中提取ID和分块数据
      val id = row.get(0).asInstanceOf[Int]
      val chunk = row.get(1).asInstanceOf[Array[Byte]]
//      println("getting Blobs...")
      // 将分块添加到对应的Blob
      blobMap.getOrElseUpdate(id.toString, ArrayBuffer()) += chunk
      // 每处理500行清理已完成的Blob
      if (processedRows % 500 == 0) {
        cleanCompletedBlobs(blobMap)
      }
    }
    // 最后清理所有剩余Blob
    cleanCompletedBlobs(blobMap)

    // 生成Blob迭代器（惰性实现）
    new Iterator[Row] {
      private val idsIterator = blobMap.keys.iterator

      override def hasNext: Boolean = idsIterator.hasNext

      override def next(): Row = {
        val id = idsIterator.next()
        val chunks = blobMap.remove(id).get.toSeq
        Row(new Blob(id, chunks))
      }
    }
  }

  /** 清理已收集所有分块的Blob */
  private def cleanCompletedBlobs(blobMap: mutable.Map[String, ArrayBuffer[Array[Byte]]]): Unit = {
    // 在实际实现中，这里可以根据服务端的结束标记清除完整文件
  }
  /** 优化的流式分块处理器（处理大规模文件） */
  def getBlobsStreaming(source: String, ops: List[DFOperation]): Iterator[Blob] = {
    new BlobStreamIterator(source, ops)
  }
  /** 用于流式处理BLOB的迭代器实现 */
  private class BlobStreamIterator(source: String, ops: List[DFOperation])
    extends Iterator[Blob] {

    private val rowIterator = getRows(source, ops).buffered
    private val idVectorName = "id"
    private val chunkVectorName = "chunk"
    private var currentId: String = _
    private var chunksBuffer = ArrayBuffer[Array[Byte]]()

    override def hasNext: Boolean = rowIterator.hasNext

    override def next(): Blob = {
      if (!rowIterator.hasNext)
        throw new NoSuchElementException("No more blobs available")

      // 清空前一个Blob的缓冲区
      chunksBuffer.clear()

      // 获取第一个分块
      val firstRow = rowIterator.next()
      currentId = firstRow.getAs[String](idVectorName)
      chunksBuffer += firstRow.getAs[Array[Byte]](chunkVectorName)

      // 收集同一ID的所有分块
      while (rowIterator.hasNext && rowIterator.head.getAs[String](idVectorName) == currentId) {
        chunksBuffer += rowIterator.next().getAs[Array[Byte]](chunkVectorName)
      }

      new Blob(currentId, chunksBuffer.toSeq)
    }
  }


  def close(): Unit = {
    flightClient.close()
  }

}

// 表示完整的二进制文件
class Blob(val id: String, val chunks: Seq[Array[Byte]]) extends Serializable {
  /** 获取完整的文件内容 */
  def content: Array[Byte] = chunks.foldLeft(Array[Byte]())((acc, chunk) => acc ++ chunk)

  /** 获取文件大小 */
  def size: Int = chunks.map(_.length).sum

  /** 获取分块数量 */
  def chunkCount: Int = chunks.size

  /** 获取分块迭代器 */
  def chunkIterator: Iterator[Array[Byte]] = chunks.iterator

  override def toString: String = s"Blob(id=$id, size=$size, chunks=$chunkCount)"
}