package link.rdcn.user.exception;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.FlightRuntimeException;

import static link.rdcn.user.exception.ErrorCode.USER_NOT_FOUND;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:06
 * @Modified By:
 */
public abstract class AuthException extends StatusRuntimeException {
    public AuthException(Status status) {
        super(status);
    }
    public AuthException(Status status, Metadata metadata) {
        super(status, metadata);
    }

    public static String getErrorCode(FlightRuntimeException e) {
        return e.status().metadata().get("error-code");
    }


}