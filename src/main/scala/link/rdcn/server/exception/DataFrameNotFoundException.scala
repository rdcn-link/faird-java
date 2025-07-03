package link.rdcn.server.exception

import io.grpc.{Metadata, Status}
import link.rdcn.ErrorCode
import link.rdcn.util.ScalaExtensions.TapAny

class DataFrameNotFoundException(dataFrameName: String) extends ServerException (
      ErrorCode.DATAFRAME_NOT_EXIST,
      Status.INTERNAL,
      new Metadata().tap { metadata =>
        metadata.put(
          Metadata.Key.of("error-code", Metadata.ASCII_STRING_MARSHALLER),
          ErrorCode.DATAFRAME_NOT_EXIST.code.toString
        )
      }
)