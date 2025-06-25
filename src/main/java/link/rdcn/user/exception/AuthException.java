package link.rdcn.user.exception;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:06
 * @Modified By:
 */
public abstract class AuthException extends Exception {
    public AuthException(String message) {
        super(message);
    }
}