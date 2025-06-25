package link.rdcn.user;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:01
 * @Modified By:
 */
public class TokenAuth extends Credentials {
    private final String token;

    public TokenAuth(String token) {
        this.token = token;
    }

    public String getToken() {
        return token;
    }
}
