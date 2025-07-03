package link.rdcn.server.exception

import io.grpc.{Metadata, Status}
import link.rdcn.ErrorCode
import link.rdcn.ErrorCode.DATAFRAME_ACCESS_DENIED
import link.rdcn.util.ScalaExtensions.TapAny

class DataFrameAccessDeniedException (dataFrameName: String)
  extends AuthException(
    DATAFRAME_ACCESS_DENIED,
    Status.PERMISSION_DENIED.withDescription(dataFrameName + " is not accessible")
  )
