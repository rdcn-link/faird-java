package link.rdcn.user;

import link.rdcn.server.exception.AuthorizationException;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 10:59
 * @Modified By:
 */
public interface AuthProvider {

    /**
     * 用户认证，成功返回认证后的保持用户登录状态的凭证
     */
    AuthenticatedUser authenticate(Credentials credentials) throws AuthorizationException;

    /**
     * 判断用户是否具有某项权限
     */
    boolean checkPermission(AuthenticatedUser user, String dataFrameName, java.util.List<DataOperationType> opList);
}


