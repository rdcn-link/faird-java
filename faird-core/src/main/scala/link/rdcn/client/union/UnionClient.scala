package link.rdcn.client.union

import link.rdcn.client.UrlValidator
import link.rdcn.client.dacp.FairdClient
import link.rdcn.client.dftp.DftpClient
import link.rdcn.struct.{DFRef, DataFrame}
import link.rdcn.user.Credentials

import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/28 09:23
 * @Modified By:
 */
private class UnionClient(url: String, port: Int, useTLS: Boolean = false) extends FairdClient(url, port, useTLS){

  private val endpoints = mutable.ListBuffer[Endpoint]()
  private val endpointClientsMap = mutable.Map[String, DftpClient]()

  def addEndpoint(endpoints: Endpoint*): Unit = {
    endpoints.foreach(endpoint => {
      this.endpointClientsMap.put(endpoint.url, endpoint.getDftpClient())
      this.endpoints.append(endpoint)
    })
  }

  override def get(url: String): DataFrame = {
    val baseUrl = UrlValidator.extractBase(url) match {
      case Some((baseUrl , _ ,_)) => baseUrl
      case None => throw new IllegalArgumentException(s"Invalid URL $url")
    }
    val dacpClient = endpointClientsMap.get(baseUrl).getOrElse(throw new IllegalArgumentException(s"No dacpClient available for the requested URL $url"))
    dacpClient.get(url)
  }

  override def getDataSetInfoMap(): Map[String, (String, String, DFRef)] = {
    val result = mutable.Map[String, (String, String, DFRef)]()
    endpoints.foreach(endpoint => {
      get(endpoint.url+"/listDataSets").mapIterator(rows => rows.foreach(row => {
        result.put(row.getAs[String](0), (row.getAs[String](1), row.getAs[String](2), row.getAs[DFRef](3)))
      }))
    })
    result.toMap
  }

  override def getHostInfoMap(): Map[String, (String, String)] = {
    val result = mutable.Map[String, (String, String)]()
    endpoints.foreach(endpoint => {
      get(endpoint.url+"/listHostInfo").mapIterator(iter => iter.foreach(row => {
        result.put(row.getAs[String](0),(row.getAs[String](1), row.getAs[String](2)))
      }))
    })
    result.toMap
  }
}

object UnionClient {
  def connect(endpoints: Seq[Endpoint]): Unit = {
    val unionClient = new UnionClient("",0)
    unionClient.addEndpoint(endpoints :_*)
  }
}

case class Endpoint(
                     name: String,
                     host: String,
                     port: Int,
                     useTLS: Boolean = false,
                     credentials: Credentials = Credentials.ANONYMOUS
                   ){
  def url: String = s"${FairdClient.protocolSchema}://${host}:${port}"
  def getDftpClient(): DftpClient = {
    if(useTLS)
      FairdClient.connectTLS(url,credentials)
    else
      FairdClient.connect(url,credentials)
  }
}
