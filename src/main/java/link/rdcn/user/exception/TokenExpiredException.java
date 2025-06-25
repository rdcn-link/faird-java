package link.rdcn.user.exception;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:06
 * @Modified By:
 */
public class TokenExpiredException extends AuthException {
    public TokenExpiredException() {
        super("Token 已过期");
    }
}
