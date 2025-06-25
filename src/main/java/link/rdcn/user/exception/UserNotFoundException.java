package link.rdcn.user.exception;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:06
 * @Modified By:
 */
public class UserNotFoundException extends AuthException {
    public UserNotFoundException() {
        super("用户不存在");
    }
}