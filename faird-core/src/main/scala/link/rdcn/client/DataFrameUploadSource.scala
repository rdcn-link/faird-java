package link.rdcn.client

import link.rdcn.struct.{StructType, Row}

trait DataFrameUploadSource {

  def schema: StructType

  def stream: Iterator[Row]

}
