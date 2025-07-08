package link.rdcn.provider

import link.rdcn.struct.{Row, StructType}
/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 17:46
 * @Modified By:
 */

trait DataStreamSource {
  def rowCount: Long
  def schema: StructType
  def iterator: Iterator[Row]
}



