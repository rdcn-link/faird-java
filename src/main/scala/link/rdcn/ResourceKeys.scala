package link.rdcn

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/16 14:13
 * @Modified By:
 */
object ResourceKeys {
  // === CPU 资源 ===
  val CpuCores        = "cpu.cores"           // CPU核心数
  val CpuUsagePercent = "cpu.usage.percent"   // CPU使用率 (%)

  // === JVM 内存 ===
  val JvmMaxMemoryMB     = "jvm.memory.max.mb"     // 最大可用内存
  val JvmTotalMemoryMB   = "jvm.memory.total.mb"   // 已分配内存
  val JvmUsedMemoryMB    = "jvm.memory.used.mb"    // 已使用内存
  val JvmFreeMemoryMB    = "jvm.memory.free.mb"    // 空闲内存

  // === 系统物理内存 ===
  val SystemMemoryTotalMB = "system.memory.total.mb"  // 总内存
  val SystemMemoryUsedMB  = "system.memory.used.mb"   // 已使用
  val SystemMemoryFreeMB  = "system.memory.free.mb"   // 空闲
}
