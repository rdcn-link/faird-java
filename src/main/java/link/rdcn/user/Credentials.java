package link.rdcn.user;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:00
 * @Modified By:
 */
public interface Credentials extends Serializable {

    public static final Credentials ANONYMOUS = AnonymousCredentialsHolder.ANONYMOUS_INSTANCE;

    // 标识是否为匿名
    boolean isAnonymous();

    @Override
    String toString();

    @Override
    boolean equals(Object obj);

    @Override
    int hashCode();

    final class AnonymousCredentials implements Credentials {
        // 私有构造器，确保外部不能直接创建实例
        AnonymousCredentials() {
        }

        @Override
        public boolean isAnonymous() {
            return true; // 匿名凭证总是返回true
        }

        // 匿名凭证的toString已经由接口默认方法处理，但为了明确性可以重写
        @Override
        public String toString() {
            return "AnonymousCredentials";
        }

        // 对于单例，equals通常只需判断是否是同一个实例
        @Override
        public boolean equals(Object obj) {
            return this == obj || (obj instanceof Credentials && ((Credentials) obj).isAnonymous());
        }

        // 对于单例，hashCode应该是一个固定值
        @Override
        public int hashCode() {
            return Objects.hash("ANONYMOUS_CREDENTIALS_HASH"); // 确保与equals一致的哈希值
        }
    }


    // 确保线程安全和懒加载
    class AnonymousCredentialsHolder {
        private static final AnonymousCredentials ANONYMOUS_INSTANCE = new AnonymousCredentials();
    }
}

