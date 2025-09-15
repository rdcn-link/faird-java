package link.rdcn.client.exception

import link.rdcn.ErrorCode
import link.rdcn.util.ExceptionHandler

class ClientException(errorCode: ErrorCode) extends Exception {
  def getErrorCode: ErrorCode = errorCode
}

object ClientException {
  def apply(e: Exception): ClientException =
    new ClientException(ExceptionHandler.getErrorCode(e))
}