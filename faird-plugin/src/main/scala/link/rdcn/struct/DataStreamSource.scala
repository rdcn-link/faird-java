package link.rdcn.struct

import link.rdcn.struct.{Row, StructType}
import link.rdcn.util.ClosableIterator

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 17:46
 * @Modified By:
 */

trait DataStreamSource {
  def rowCount: Long

  def schema: StructType

  def iterator: ClosableIterator[Row]
}



