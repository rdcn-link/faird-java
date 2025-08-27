package link.rdcn.util

import java.io.InputStream
/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/26 16:56
 * @Modified By:
 */
class IteratorInputStream(it: Iterator[Array[Byte]]) extends InputStream {
  private var currentChunk: Array[Byte] = Array.emptyByteArray
  private var index: Int = 0

  /** 从流中读取一个字节 */
  override def read(): Int = {
    if (currentChunk == null) return -1
    if (index >= currentChunk.length) {
      if (it.hasNext) {
        currentChunk = it.next()
        index = 0
        read()
      } else {
        currentChunk = null
        -1
      }
    } else {
      val b = currentChunk(index) & 0xff
      index += 1
      b
    }
  }

  /** 从流中读取多个字节到缓冲区 */
  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (currentChunk == null) return -1
    var totalRead = 0
    while (totalRead < len) {
      if (index >= currentChunk.length) {
        if (it.hasNext) {
          currentChunk = it.next()
          index = 0
        } else {
          return if (totalRead == 0) -1 else totalRead
        }
      }
      val bytesToCopy = math.min(len - totalRead, currentChunk.length - index)
      System.arraycopy(currentChunk, index, b, off + totalRead, bytesToCopy)
      index += bytesToCopy
      totalRead += bytesToCopy
    }
    totalRead
  }
}

