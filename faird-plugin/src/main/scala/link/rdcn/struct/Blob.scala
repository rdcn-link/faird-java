package link.rdcn.struct

import org.apache.commons.io.IOUtils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileOutputStream, InputStream}
import java.nio.file.Paths

// 表示完整的二进制文件
class Blob(val chunkIterator: Iterator[Array[Byte]], val name: String) extends Serializable {
  // 缓存加载后的完整数据
  private var _content: Option[Array[Byte]] = None
  // 缓存文件大小（独立于_content，避免获取大数组长度）
  private var _size: Option[Long] = None

  private var _memoryReleased: Boolean = false

  private def loadLazily(): Unit = {
    val byteStream = new ByteArrayOutputStream()
    var totalSize: Long = 0L
    var chunkCount = 0
    try {
      if (_content.isEmpty && _size.isEmpty) {
        while (chunkIterator.hasNext) {
          val chunk = chunkIterator.next()
          totalSize += chunk.length
          chunkCount += 1
          byteStream.write(chunk)

        }
        _content = Some(byteStream.toByteArray)
        _size = Some(totalSize)
      }
    }
    catch {
      case e: OutOfMemoryError => {
        _size = Some(offerStream(inputStream => {
          val outputStream = new FileOutputStream(Paths.get("src", "test", "demo", "data", "output", name).toFile)
          IOUtils.copy(inputStream, outputStream)
        }))
        _content = None
      }
    } finally {
      byteStream.close()
    }


  }


  /** 获取完整的文件内容 */
  def toBytes: Array[Byte] = {
    if (_memoryReleased) {
      throw new IllegalStateException("Blob toBytes memory has been released")
    }
    if (_content.isEmpty) loadLazily()
    _content.get
  }


  /** 获取文件大小 */
  def size: Long = {
    if (_size.isEmpty) loadLazily()
    _size.get
  }

  /** 释放content占用的内存 */
  def releaseMemory(): Unit = {
    _content = None
    _memoryReleased = true
    System.gc()
  }

  // 获得 `InputStream`（适合流式读取 `toBytes`）
  def offerStream[T](consume: InputStream => T): T = {
    if (_memoryReleased) throw new IllegalStateException("Blob toBytes memory has been released")
    if (_content.isEmpty) loadLazily()
    val inputStream = new ByteArrayInputStream(_content.get)
    try {
      consume(inputStream)
    } finally {
      inputStream.close()
    }
  }

  override def toString: String = {
    loadLazily()
    s"Blob[$name]"
  }
}