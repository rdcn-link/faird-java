package link.rdcn.user;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:00
 * @Modified By:
 */
public class Credentials implements Serializable {

    // 匿名用户单例
    public static final Credentials ANONYMOUS = new Credentials();

    // 标识是否为匿名
    public boolean isAnonymous() {
        return this == ANONYMOUS;
    }

    @Override
    public String toString() {
        return isAnonymous() ? "AnonymousCredentials" : "Credentials";
    }

    @Override
    public boolean equals(Object obj) {
        // 所有匿名对象视为等同
        return obj instanceof Credentials && (this == ANONYMOUS && obj == ANONYMOUS || super.equals(obj));
    }

    @Override
    public int hashCode() {
        return Objects.hash("ANONYMOUS");
    }
}
