package link.rdcn.client

import link.rdcn.user.{Credentials, TokenAuth, UsernamePassword}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 18:17
 * @Modified By:
 */


class DacpClient(url: String, port: Int, user: String = null, password: String = null, token: String = null) {
  private val client = new ArrowFlightClient(url, port)
  if (token == null)
    client.login(new UsernamePassword(user, password))
  else
    client.login(new TokenAuth(token))

  def open(dataFrameName: String): RemoteDataFrameImpl = {
    RemoteDataFrameImpl(dataFrameName, List.empty, client)
  }
  def listDataSetNames(): Seq[String] = client.listDataSetNames
  def listDataFrameNames(dsName: String): Seq[String] = client.listDataFrameNames(dsName)

  def getDataSetMetaData(dsName: String): String = client.getDataSetMetaData(dsName)
}

