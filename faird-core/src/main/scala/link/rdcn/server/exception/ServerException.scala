package link.rdcn.server.exception

import io.grpc.{Metadata, Status, StatusRuntimeException}
import link.rdcn.ErrorCode
import link.rdcn.util.ScalaExtensions.TapAny

class ServerException(errorCode: ErrorCode = ErrorCode.ERROR_CODE_NOT_EXIST,
                      status: Status = Status.INTERNAL,
                      metadata: Metadata = new Metadata().tap { metadata =>
                        metadata.put(
                          Metadata.Key.of("error-code", Metadata.ASCII_STRING_MARSHALLER),
                          ErrorCode.ERROR_CODE_NOT_EXIST.code.toString
                        )
                      })
  extends StatusRuntimeException(status, metadata) {
  def getErrorCode: ErrorCode = errorCode
}

