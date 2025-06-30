package link.rdcn.user.exception;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

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
}