package link.rdcn.user.exception;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:06
 * @Modified By:
 */
public class UserNotFoundException extends AuthException {
    private static final io.grpc.Status status = io.grpc.Status.NOT_FOUND
            .withDescription("用户不存在!");

    public UserNotFoundException() {
        super(status);
    }
}