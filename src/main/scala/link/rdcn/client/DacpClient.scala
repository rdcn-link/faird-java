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
  if (user != null && password != null && token == null)
    client.login(new UsernamePassword(user, password))
  else if(user == null && password == null && token != null)
    client.login(new TokenAuth(token))
  else
    throw new IllegalArgumentException("user and password or token must be provided")

  def open(dataFrameName: String): RemoteDataFrameImpl = {
    RemoteDataFrameImpl(dataFrameName, List.empty, client)
  }
  def listDataSetNames(): Seq[String] = client.listDataSetNames
  def listDataFrameNames(dsName: String): Seq[String] = client.listDataFrameNames(dsName)

  def getDataSetMetaData(dsName: String): String = client.getDataSetMetaData(dsName)
}

