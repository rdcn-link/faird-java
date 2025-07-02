package link.rdcn.user.exception;

import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.Metadata;
import io.grpc.protobuf.StatusProto;

import static link.rdcn.user.exception.ErrorCode.USER_NOT_FOUND;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:06
 * @Modified By:
 */
public class InvalidCredentialsException extends AuthException {
    private static final io.grpc.Status status = io.grpc.Status.INTERNAL;
    private static final Metadata metadata = new Metadata();

    static {
        metadata.put(
                Metadata.Key.of("error-code", Metadata.ASCII_STRING_MARSHALLER),
                String.valueOf(ErrorCode.INVALID_CREDENTIALS.getCode())
        );
    }

    public InvalidCredentialsException() {
        super(status, metadata);
    }
}