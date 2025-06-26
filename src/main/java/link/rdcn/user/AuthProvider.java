package link.rdcn.user;

import link.rdcn.user.exception.AuthException;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 10:59
 * @Modified By:
 */
public interface AuthProvider {
    void putAuthenticatedUser(String token ,AuthenticatedUser user);


    /**
     * 用户认证，成功返回认证后的用户信息，失败抛出 AuthException 异常
     */
    AuthenticatedUser authenticate(Credentials credentials) throws AuthException;

    /**
     * 判断用户是否具有某项权限
     */
    boolean authorize(AuthenticatedUser user, String dataFrameName);
}
