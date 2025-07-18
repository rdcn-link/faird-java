/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/18 18:05
 * @Modified By:
 */
package link.rdcn;

public class TestFairdConfigKeys {
    public static final String faird_host_name = FairdConfigKeys.faird_host_name(); // 调用 Scala 的方法
    public static final String faird_host_port = FairdConfigKeys.faird_host_port();
    public static final String faird_host_title = FairdConfigKeys.faird_host_title();
    public static final String faird_host_position = FairdConfigKeys.faird_host_position();
    public static final String faird_host_domain = FairdConfigKeys.faird_host_domain();
    public static final String faird_tls_enabled = FairdConfigKeys.faird_tls_enabled();
    public static final String faird_tls_cert_path = FairdConfigKeys.faird_tls_cert_path();
    public static final String faird_tls_key_path = FairdConfigKeys.faird_tls_key_path();

    // 私有构造函数，防止实例化
    private void TestFairdConfigKeysJava() {}
}
