package link.rdcn.server.dftp

import link.rdcn.client.UrlValidator
import link.rdcn.optree.{ExecutionContext, Operation}
import link.rdcn.struct.DataFrame
/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/26 17:14
 * @Modified By:
 */
trait DftpServiceHandler {

  def doGet(request: GetRequest, response: GetResponse): Unit

  def doPut(request: PutRequest, putResponse: PutResponse): Unit

  def doAction(request: ActionRequest, response: ActionResponse): Unit

  def doCook(request: CookRequest, response: CookResponse): Unit = {
    val operation = request.getOperation
    try{
      response.sendDataFrame(operation.execute(new ExecutionContext {
        override def loadSourceDataFrame(dataFrameNameUrl: String): Option[DataFrame] = {
          var resultDataFrame: Option[DataFrame] = None
          val baseUrlAndPath: (Option[String], String) = UrlValidator.extractBaseUrlAndPath(dataFrameNameUrl) match {
            case Right((baseUrl, path)) => (Some(baseUrl), path)
            case Left(message) => (None, dataFrameNameUrl)
          }
          val getRequest = new GetRequest {
            override def getRequestedPath(): String = baseUrlAndPath._2
            override def getRequestedBaseUrl(): Option[String] = baseUrlAndPath._1
          }
          val getResponse = new GetResponse {
            override def sendDataFrame(dataFrame: DataFrame): Unit = resultDataFrame = Some(dataFrame)

            override def sendError(code: Int, message: String): Unit = response.sendError(code, message)
          }
          doGet(request = getRequest, response = getResponse)
          resultDataFrame
        }
      }))
    }catch {
      case e: Exception => response.sendError(500, e.getMessage)
    }
  }

}
