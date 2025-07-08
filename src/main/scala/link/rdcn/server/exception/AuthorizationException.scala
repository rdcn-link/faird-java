package link.rdcn.server.exception

import io.grpc.{Metadata, Status}
import io.grpc.StatusRuntimeException
import link.rdcn.ErrorCode
import link.rdcn.util.ScalaExtensions.TapAny

class AuthorizationException(errorCode: ErrorCode, status: Status = Status.UNAUTHENTICATED)
  extends ServerException(
    errorCode,
    status,
    new Metadata().tap { metadata =>
      metadata.put(
        Metadata.Key.of("error-code", Metadata.ASCII_STRING_MARSHALLER),
        errorCode.code.toString
      )
    }
  )
