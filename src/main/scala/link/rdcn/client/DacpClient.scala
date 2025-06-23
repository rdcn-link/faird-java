package link.rdcn.client

import link.rdcn.UsernamePassword
import org.apache.jena.rdf.model.Model
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection.{Schema, schemaFor}
import org.apache.spark.sql.types.StructType

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 18:17
 * @Modified By:
 */


class DacpClient(url: String, port: Int, user: String = null, password: String = null) {
  private val client = new ArrowFlightClient(url, port)
//  def execute(source: String, ops: List[DFOperation]): Iterator[Row] = client.getRows(source, ops)
  def open(dataFrameName: String): RemoteDataFrameImpl = {
    val source = DataAccessRequest(dataFrameName, UsernamePassword(user, password))
    RemoteDataFrameImpl(source, List.empty, client)
  }
  def listDataSetNames(): Seq[String] = client.listDataSetNames
  def listDataFrameNames(dsName: String): Seq[String] = client.listDataFrameNames(dsName)

  def getDataSetMetaData(dsName: String): Model = ???
}

