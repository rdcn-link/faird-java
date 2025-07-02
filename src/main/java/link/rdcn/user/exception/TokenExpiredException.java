package link.rdcn.user.exception;

import io.grpc.Metadata;


/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:06
 * @Modified By:
 */
public class TokenExpiredException extends AuthException {
    private static final io.grpc.Status status = io.grpc.Status.INTERNAL;
    private static final Metadata metadata = new Metadata();

    static {
        metadata.put(
                Metadata.Key.of("error-code", Metadata.ASCII_STRING_MARSHALLER),
                String.valueOf(ErrorCode.TOKEN_EXPIRED.getCode())
        );
    }

    public TokenExpiredException() {
        super(status, metadata);
    }

}
