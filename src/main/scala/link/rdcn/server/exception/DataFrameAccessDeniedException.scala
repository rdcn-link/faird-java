package link.rdcn.server.exception

import io.grpc.Status
import link.rdcn.ErrorCode.DATAFRAME_ACCESS_DENIED

class DataFrameAccessDeniedException (dataFrameName: String)
  extends AuthorizationException(
    DATAFRAME_ACCESS_DENIED,
    Status.PERMISSION_DENIED.withDescription("DataFrame is not accessible")
  )
