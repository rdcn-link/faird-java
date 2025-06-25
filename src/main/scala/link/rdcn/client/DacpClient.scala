package link.rdcn.client

import link.rdcn.user.UsernamePassword

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
    val source = DataAccessRequest(dataFrameName, new UsernamePassword(user, password))
    RemoteDataFrameImpl(source, List.empty, client)
  }
  def listDataSetNames(): Seq[String] = client.listDataSetNames
  def listDataFrameNames(dsName: String): Seq[String] = client.listDataFrameNames(dsName)

  def getDataSetMetaData(dsName: String): String = client.getMetaData(dsName)
}

