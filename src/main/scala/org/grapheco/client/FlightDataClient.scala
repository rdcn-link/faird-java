package org.grapheco.client

import org.apache.arrow.flight.{AsyncPutListener, FlightClient, FlightDescriptor, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.Row
import org.grapheco.SimpleSerializer

import java.util.UUID
import java.io.ByteArrayOutputStream
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
    val iter: Iterator[Seq[Seq[Any]]] = new Iterator[Seq[Seq[Any]]] {
      override def hasNext: Boolean = flightStream.next()


      override def next(): Seq[Seq[Any]] = {
        val vectorSchemaRootReceived = flightStream.getRoot
        val rowCount = vectorSchemaRootReceived.getRowCount
        val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
//        var it = Seq.range(0, rowCount).toIterator
        Seq.range(0, rowCount).map(index => {
          val rowMap = fieldVectors.map(vec => {
            if (vec.isNull(index)) (vec.getName, null)
            else vec match {
              case v: org.apache.arrow.vector.IntVector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.VarCharVector => (vec.getName, new String(v.get(index)))
              case v: org.apache.arrow.vector.Float8Vector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.BitVector => (vec.getName, v.get(index) == 1)
              case v: org.apache.arrow.vector.VarBinaryVector => (vec.getName, v.get(index))
              case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getClass}")
            }
          }).toMap
          val r: Seq[Any] = rowMap.toSeq.map(x => x._2)
//                    Row(rowMap.toSeq.map(x => x._2): _*)
          r
        })
      }
    }
    val flatIter: Iterator[Seq[Any]] = iter.flatMap(rows=>rows)
    val seq: Seq[Any] = flatIter.next()
    var currentChunk: Array[Byte] = Array[Byte]()
    var cachedChunk: Array[Byte] = seq(2).asInstanceOf[Array[Byte]]
    var isFirstChunk: Boolean = true
    var cachedIndex: Int = seq(0).asInstanceOf[Int]
    var cachedName: String = seq(1).asInstanceOf[String]
    var currentIndex: Int = 0  // 当前块的 index
    var currentName: String = seq(1).asInstanceOf[String]
    new Iterator[Row] {
      override def hasNext: Boolean = flatIter.hasNext || cachedChunk.nonEmpty

      override def next(): Row = {

        val blobIter: Iterator[Array[Byte]] = new Iterator[Array[Byte]] {

          private var isExhausted: Boolean = false  // flatIter 是否耗尽
          // 预读取下一块的 index 和 data（如果存在）
          private def readNextChunk(): Unit = {
            if (flatIter.hasNext) {
              if(!isFirstChunk){
                val seq: Seq[Any] = flatIter.next()
                val nextIndex:Int = seq(0).asInstanceOf[Int]
                val nextName:String = seq(1).asInstanceOf[String]
                val nextChunk:Array[Byte] = seq(2).asInstanceOf[Array[Byte]]
                if (nextIndex != currentIndex) {
                  // index 变化，结束当前块
                  isExhausted = true
                  isFirstChunk=true
                  cachedIndex=nextIndex
                  cachedName=nextName
                  cachedChunk=nextChunk
                } else {

                  currentChunk=nextChunk
                  currentName=nextName
                }
                currentIndex = nextIndex
                currentName = nextName
              } else {
                currentIndex=cachedIndex
                currentName=cachedName
                currentChunk=cachedChunk
                isExhausted=false
                isFirstChunk=false
              }
//              println(currentIndex)

            } else {
              // flatIter 耗尽
              if(cachedChunk.nonEmpty)
                currentChunk=cachedChunk
              isExhausted = true
            }
          }
          // hasNext: 检查是否还有块（可能预读取）
          override def hasNext: Boolean = {
            if (currentChunk.isEmpty && !isExhausted) {
              readNextChunk() // 如果当前块为空且迭代器未结束，尝试预读取
            }
            !isExhausted || currentChunk.nonEmpty
          }
          // next: 返回当前块（已由 hasNext 预加载）
          override def next(): Array[Byte] = {

            if(!isFirstChunk) {
              val chunk = currentChunk
              currentChunk = Array.empty[Byte]
              if(!flatIter.hasNext)
                cachedChunk = Array.empty[Byte]
              chunk
                // 手动清空
            } else {
              val chunk = cachedChunk
              cachedChunk = Array.empty[Byte]
              currentChunk = Array.empty[Byte]
              chunk
            }

          }
        }
        Row(currentIndex+1,currentName,new Blob(blobIter))
//          Row(iter.next())
      }
    }
  }

  def getFilesRows(source: String, ops: List[DFOperation]): Iterator[Row] = {
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
    if (ops.length == 0) {
      DFOperationVector.allocateNew(1)
      vectorSchemaRoot.setRowCount(1)
    } else {
      DFOperationVector.allocateNew(ops.length)
      for (i <- 0 to ops.length - 1) {
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
    val flightInfoSchema = flightInfo.getSchema
    val isBinaryColumn = if (flightInfoSchema.getFields.size() < 3) false
    else schema.getFields.get(2).getType match {
      case _: ArrowType.Binary => true
      case _ => false
    }
    val flightStream = flightClient.getStream(flightInfo.getEndpoints.get(0).getTicket)
    val iter: Iterator[Seq[Seq[Any]]] = new Iterator[Seq[Seq[Any]]] {
      override def hasNext: Boolean = flightStream.next()


      override def next(): Seq[Seq[Any]] = {
        val vectorSchemaRootReceived = flightStream.getRoot
        val rowCount = vectorSchemaRootReceived.getRowCount
        val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
        //        var it = Seq.range(0, rowCount).toIterator
        Seq.range(0, rowCount).map(index => {
          val rowMap = fieldVectors.map(vec => {
            if (vec.isNull(index)) (vec.getName, null)
            else vec match {
              case v: org.apache.arrow.vector.IntVector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.VarCharVector => (vec.getName, new String(v.get(index)))
              case v: org.apache.arrow.vector.Float8Vector => (vec.getName, v.get(index))
              case v: org.apache.arrow.vector.BitVector => (vec.getName, v.get(index) == 1)
              case v: org.apache.arrow.vector.VarBinaryVector => (vec.getName, v.get(index))
              case _ => throw new UnsupportedOperationException(s"Unsupported vector type: ${vec.getClass}")
            }
          }).toMap
          val r: Seq[Any] = rowMap.toSeq.map(x => x._2)
          //                    Row(rowMap.toSeq.map(x => x._2): _*)
          r
        })
      }
    }
    val flatIter: Iterator[Seq[Any]] = iter.flatMap(rows => rows)
    val seq: Seq[Any] = flatIter.next()
    var currentChunk: Array[Byte] = Array[Byte]()
    var cachedChunk: Array[Byte] = seq(2).asInstanceOf[Array[Byte]]
    var isFirstChunk: Boolean = true
    var cachedIndex: Int = seq(0).asInstanceOf[Int]
    var cachedName: String = seq(1).asInstanceOf[String]
    var currentIndex: Int = 0 // 当前块的 index
    var currentName: String = seq(1).asInstanceOf[String]
    if (!isBinaryColumn) {
      // 第三列不是binary类型，直接返回Row(Seq[Any])
      flatIter.map(seq => Row.fromSeq(seq))
    } else {
      new Iterator[Row] {
        override def hasNext: Boolean = flatIter.hasNext || cachedChunk.nonEmpty

        override def next(): Row = {

          val blobIter: Iterator[Array[Byte]] = new Iterator[Array[Byte]] {

            private var isExhausted: Boolean = false // flatIter 是否耗尽

            // 预读取下一块的 index 和 data（如果存在）
            private def readNextChunk(): Unit = {
              if (flatIter.hasNext) {
                if (!isFirstChunk) {
                  val seq: Seq[Any] = flatIter.next()
                  val nextIndex: Int = seq(0).asInstanceOf[Int]
                  val nextName: String = seq(1).asInstanceOf[String]
                  val nextChunk: Array[Byte] = seq(2).asInstanceOf[Array[Byte]]
                  if (nextIndex != currentIndex) {
                    // index 变化，结束当前块
                    isExhausted = true
                    isFirstChunk = true
                    cachedIndex = nextIndex
                    cachedName = nextName
                    cachedChunk = nextChunk
                  } else {

                    currentChunk = nextChunk
                    currentName = nextName
                  }
                  currentIndex = nextIndex
                  currentName = nextName
                } else {
                  currentIndex = cachedIndex
                  currentName = cachedName
                  currentChunk = cachedChunk
                  isExhausted = false
                  isFirstChunk = false
                }
                //              println(currentIndex)

              } else {
                // flatIter 耗尽
                if (cachedChunk.nonEmpty)
                  currentChunk = cachedChunk
                isExhausted = true
              }
            }

            // hasNext: 检查是否还有块（可能预读取）
            override def hasNext: Boolean = {
              if (currentChunk.isEmpty && !isExhausted) {
                readNextChunk() // 如果当前块为空且迭代器未结束，尝试预读取
              }
              !isExhausted || currentChunk.nonEmpty
            }

            // next: 返回当前块（已由 hasNext 预加载）
            override def next(): Array[Byte] = {

              if (!isFirstChunk) {
                val chunk = currentChunk
                currentChunk = Array.empty[Byte]
                if (!flatIter.hasNext)
                  cachedChunk = Array.empty[Byte]
                chunk
                // 手动清空
              } else {
                val chunk = cachedChunk
                cachedChunk = Array.empty[Byte]
                currentChunk = Array.empty[Byte]
                chunk
              }

            }
          }
          Row(currentIndex + 1, currentName, new Blob(blobIter))
          //          Row(iter.next())
        }
      }
    }
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
        Row(new Blob(chunks.toIterator))
      }
    }
  }


  /** 清理已收集所有分块的Blob */
  private def cleanCompletedBlobs(blobMap: mutable.Map[String, ArrayBuffer[Array[Byte]]]): Unit = {
    // 在实际实现中，这里可以根据服务端的结束标记清除完整文件
  }
  /** 优化的流式分块处理器（处理大规模文件） */
//  def getBlobsStreaming(source: String, ops: List[DFOperation]): Iterator[Blob] = {
//    new BlobStreamIterator(source, ops)
//  }
  /** 用于流式处理BLOB的迭代器实现 */
//  private class BlobStreamIterator(source: String, ops: List[DFOperation])
//    extends Iterator[Blob] {
//
//    private val rowIterator = getRows(source, ops).buffered
//    private val idVectorName = "id"
//    private val chunkVectorName = "chunk"
//    private var currentId: String = _
//    private var chunksBuffer = ArrayBuffer[Array[Byte]]()
//erator.head.getAs[String](idVectorName) == currentId) {
//        chunksBuffer += rowIterator.next().getAs[Array[Byte]](chunkVectorName)
//      }
//
//      new Blob(currentId, chunksBuffer.toSeq)
//    }
//  }

  //    override def hasNext: Boolean = rowIterator.hasNext
  //
  //    override def next(): Blob = {
  //      if (!rowIterator.hasNext)
  //        throw new NoSuchElementException("No more blobs available")
  //
  //      // 清空前一个Blob的缓冲区
  //      chunksBuffer.clear()
  //
  //      // 获取第一个分块
  //      val firstRow = rowIterator.next()
  //      currentId = firstRow.getAs[String](idVectorName)
  //      chunksBuffer += firstRow.getAs[Array[Byte]](chunkVectorName)
  //
  //      // 收集同一ID的所有分块
  //      while (rowIterator.hasNext && rowIt

  def close(): Unit = {
    flightClient.close()
  }

}

// 表示完整的二进制文件
class Blob( val chunkIterator:Iterator[Array[Byte]]) extends Serializable {
  // 缓存加载后的完整数据
  private var _content: Option[Array[Byte]] = None
  // 缓存文件大小（独立于_content，避免获取大数组长度）
  private var _size: Option[Long] = None

  private var _chunkCount: Option[Int] = None

  private var _memoryReleased: Boolean = false

  private def loadLazily(): Unit = {
//    println("loadLazily")
    if (_content.isEmpty && _size.isEmpty && _chunkCount.isEmpty) {
      val byteStream = new ByteArrayOutputStream()
      var totalSize: Long = 0L
      var chunkCount = 0

      while (chunkIterator.hasNext) {
        val chunk = chunkIterator.next()
//        println(chunk.mkString("Array(", ", ", ")"))
        totalSize += chunk.length
        chunkCount+=1
        byteStream.write(chunk)
        byteStream.reset()

      }
//      println("loaded Lazily")
      _content = Some(byteStream.toByteArray)
      _size = Some(totalSize)
      _chunkCount = Some(chunkCount)
      byteStream.close()
    }
  }


  /** 获取完整的文件内容 */
  def content: Array[Byte] = {
    if (_memoryReleased) {
      throw new IllegalStateException("Blob content memory has been released")
    }
    if (_content.isEmpty) loadLazily()
    _content.get
  }


  /** 获取文件大小 */
  def size: Long = {
    if (_size.isEmpty) loadLazily()
    _size.get
  }

  /** 获取分块数量 */
  def chunkCount: Int = {
    if (_chunkCount.isEmpty) loadLazily()
    _chunkCount.get
  }

  /** 释放content占用的内存 */
  def releaseContentMemory(): Unit = {
    _content = None
    _memoryReleased = true
    System.gc()
  }

  /** 获取分块迭代器 */
//  def chunkIterator: Iterator[Array[Byte]] = chunkIterator

  override def toString: String = s"Blob()"
}