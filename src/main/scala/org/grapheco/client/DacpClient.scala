package org.grapheco.server

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection.{Schema, schemaFor}
import org.apache.spark.sql.types.StructType
import org.grapheco.UsernamePassword
import org.grapheco.client.{Blob, DFOperation, DataAccessRequest, FlightDataClient, InputSource, StructuredSource}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 18:17
 * @Modified By:
 */


class DacpClient(url: String, port: Int, user: String = null, password: String = null) {
  private val client = new FlightDataClient(url, port)
//  def execute(source: String, ops: List[DFOperation]): Iterator[Row] = client.getRows(source, ops)
  def open(dataFrameName: String): RemoteDataFrameImpl = {
    val source = DataAccessRequest(dataFrameName, UsernamePassword(user, password))
    RemoteDataFrameImpl(source, List.empty, client)
  }
  def listDataSetNames(): Seq[String] = client.listDataSetNames
  def listDataFrameNames(dsName: String): Seq[String] = client.listDataFrameNames(dsName)
}

