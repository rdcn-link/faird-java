package link.rdcn.user;

import scala.collection.immutable.Set;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:02
 * @Modified By:
 */
public class AuthenticatedUser {
    private final String userId;
    private final Set<String> roles;
    private final Set<String> permissions;

    public AuthenticatedUser(String userId, Set<String> roles, Set<String> permissions) {
        this.userId = userId;
        this.roles = roles;
        this.permissions = permissions;
    }

    public String getUserId() {
        return userId;
    }

    public Set<String> getRoles() {
        return roles;
    }

    public Set<String> getPermissions() {
        return permissions;
    }
}
