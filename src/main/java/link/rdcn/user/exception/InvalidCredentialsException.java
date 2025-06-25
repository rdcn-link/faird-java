package link.rdcn.user.exception;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:06
 * @Modified By:
 */
public class InvalidCredentialsException extends AuthException {
    public InvalidCredentialsException() {
        super("用户名或密码无效");
    }
}