/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/18 18:05
 * @Modified By:
 */
package link.rdcn;

public class TestFairdKeys {
    public static final String faird_host_name = ConfigKeys.FairdHostName(); // 调用 Scala 的方法
    public static final String faird_host_port = ConfigKeys.FairdHostPort();
    public static final String faird_host_title = ConfigKeys.FairdHostTitle();
    public static final String faird_host_position = ConfigKeys.FairdHostPosition();
    public static final String faird_host_domain = ConfigKeys.FairdHostDomain();
    public static final String faird_tls_enabled = ConfigKeys.FairdTlsEnabled();
    public static final String faird_tls_cert_path = ConfigKeys.FairdTlsCertPath();
    public static final String faird_tls_key_path = ConfigKeys.FairdTlsKeyPath();

    public static final String logging_file_name = ConfigKeys.LoggingFileName();
    public static final String logging_level_root = ConfigKeys.LoggingLevelRoot();
    public static final String logging_pattern_console = ConfigKeys.LoggingPatternConsole();
    public static final String logging_pattern_file = ConfigKeys.LoggingPatternFile();

    public static final String faird_cpu_cores = ResourceKeys.CpuCores();
    public static final String faird_cpu_usage_percent = ResourceKeys.CpuUsagePercent();
    public static final String faird_jvm_max_memory = ResourceKeys.JvmMaxMemory();
    public static final String faird_jvm_total_memory = ResourceKeys.JvmTotalMemory();
    public static final String faird_jvm_used_memory = ResourceKeys.JvmUsedMemory();
    public static final String faird_jvm_free_memory = ResourceKeys.JvmFreeMemory();
    public static final String faird_system_memory_total = ResourceKeys.SystemMemoryTotal();
    public static final String faird_system_memory_used = ResourceKeys.SystemMemoryUsed();
    public static final String faird_system_memory_free = ResourceKeys.SystemMemoryFree();
    // 私有构造函数，防止实例化
    private void TestFairdConfigKeysJava() {}
}
