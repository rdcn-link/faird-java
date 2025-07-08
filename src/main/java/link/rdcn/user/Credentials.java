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

    private final boolean anonymous;

    public static final Credentials ANONYMOUS = new Credentials(true);

    public Credentials() {
        this(false);
    }

    private Credentials(boolean anonymous) {
        this.anonymous = anonymous;
    }

    public boolean isAnonymous() {
        return anonymous;
    }
}
