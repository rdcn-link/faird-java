package link.rdcn.server.dftp

import link.rdcn.dftree.Operation
import link.rdcn.struct.DataFrame

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/27 09:43
 * @Modified By:
 */
trait GetRequest {
  def getRequestedUrl(): String
  def getTransformer(): Operation
}

trait GetResponse {
  def sendDataFrame(dataFrame: DataFrame): Unit
  def sendError(code: Int, message: String): Unit
}

trait ActionRequest{
  def getActionName(): String
  def getActionParameters(): Array[Byte]
  def getActionParameterMap(): Map[String, Any]
}

trait ActionResponse{
  def sendDataFrame(dataFrame: DataFrame): Unit
  def sendError(code: Int, message: String): Unit
}

trait PutRequest{
  def getDataFrame(): DataFrame
}

trait PutResponse{
  def sendBinary(bytes: Array[Byte]): Unit
  def sendMap(data: Map[String, Any]): Unit
  def sendJsonString(jsonStr: String): Unit
  def sendError(code: Int, message: String): Unit
}
