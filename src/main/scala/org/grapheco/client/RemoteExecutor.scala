package org.grapheco.server

import org.apache.spark.sql.Row
import org.grapheco.client.{Blob, DFOperation, FlightDataClient}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 18:17
 * @Modified By:
 */


class RemoteExecutor(url: String, port: Int) {
  private val client = new FlightDataClient(url, port)
  def execute(source: String, ops: List[DFOperation]): Iterator[Row] = client.getBlobs(source, ops)
  def open(dataSource: String): RemoteDataFrameImpl = {
    RemoteDataFrameImpl(dataSource, List.empty, this)
  }
}

