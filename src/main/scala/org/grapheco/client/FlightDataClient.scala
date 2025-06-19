package org.grapheco.client

import org.apache.arrow.flight.{AsyncPutListener, FlightClient, FlightDescriptor, FlightInfo, Location}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.{VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType}
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
  private val flightClient = FlightClient.builder(allocator, location).build()

  def listDataSetNames(): Seq[String] = {
    val flightInfo = flightClient.getInfo(FlightDescriptor.path("listDataSetNames"))
    getListStrByFlightInfo(flightInfo)
  }
  def listDataFrameNames(dsName: String): Seq[String] = {
    val flightInfo = flightClient.getInfo(FlightDescriptor.path(s"listDataFrameNames.$dsName"))
    getListStrByFlightInfo(flightInfo)
  }

  def getRows(request: DataAccessRequest, ops: List[DFOperation]): Iterator[Row]  = {
    //上传参数
    val paramFields: Seq[Field] = List(
      new Field("source", FieldType.nullable(new ArrowType.Binary()), null),
      new Field("DFOperation", FieldType.nullable(new ArrowType.Binary()), null)
    )
    val schema = new Schema(paramFields.asJava)
    val vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator)
    val varCharVector = vectorSchemaRoot.getVector("source").asInstanceOf[VarBinaryVector]
    val DFOperationVector = vectorSchemaRoot.getVector("DFOperation").asInstanceOf[VarBinaryVector]
    varCharVector.allocateNew(1)
    varCharVector.set(0, SimpleSerializer.serialize(request))
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
          Row(rowMap.toSeq.map(x => x._2): _*)
        })
      }
    }.flatMap(rows => rows)
  }

  def close(): Unit = {
    flightClient.close()
  }

  private def getListStrByFlightInfo(flightInfo: FlightInfo): Seq[String] = {
    val flightStream = flightClient.getStream(flightInfo.getEndpoints.get(0).getTicket)
    if(flightStream.next()){
      val vectorSchemaRootReceived = flightStream.getRoot
      val rowCount = vectorSchemaRootReceived.getRowCount
      val fieldVectors = vectorSchemaRootReceived.getFieldVectors.asScala
      Seq.range(0, rowCount).map(index  => {
        val rowMap = fieldVectors.map(vec =>{
          vec.asInstanceOf[VarCharVector].getObject(index).toString
        }).head
        rowMap
      })
    }else Seq.empty
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