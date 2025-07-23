package link.rdcn.util

import link.rdcn.Logging

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/21 17:20
 * @Modified By:
 */
class AutoClosingIterator[T](
                              underlying: Iterator[T],
                              val onClose: () => Unit
                            ) extends Iterator[T] with AutoCloseable with Logging {

  private var closed = false
  //避免多次触发close
  private var hasMore = true

  override def hasNext: Boolean = {
    if (!hasMore || closed) return false
    val more = underlying.hasNext
    if (!more) {
      hasMore = false
      close()
    }
    more
  }

  override def next(): T = {
    if (!hasNext) throw new NoSuchElementException("next on empty iterator")
    val value = underlying.next()
    if (!underlying.hasNext) {
      hasMore = false
      close()
    }
    value
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      try onClose()
      catch {
        case ex: Throwable => logger.error(s"[AutoClosingIterator] Error during close: ${ex.getMessage}")
      }
    }
  }
}

object AutoClosingIterator {
  def apply[T](underlying: Iterator[T])(onClose: => Unit): AutoClosingIterator[T] =
    new AutoClosingIterator[T](underlying, () => onClose)
}
