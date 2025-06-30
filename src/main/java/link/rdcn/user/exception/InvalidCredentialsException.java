package link.rdcn.user.exception;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:06
 * @Modified By:
 */
public class InvalidCredentialsException extends AuthException {
    private static final io.grpc.Status status = io.grpc.Status.NOT_FOUND
            .withDescription("无效的用户名/密码!");

    public InvalidCredentialsException() {
        super(status);
    }
}