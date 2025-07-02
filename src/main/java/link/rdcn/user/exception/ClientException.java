package link.rdcn.user.exception;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.FlightRuntimeException;

public class ClientException extends Exception {
    private final ErrorCode errorCode;

    public ClientException(Exception e) {
        super();
        this.errorCode = ErrorCode.fromException(e);
    }

    public ClientException(FlightRuntimeException e) {
        super();
        this.errorCode = ErrorCode.fromException(e);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
