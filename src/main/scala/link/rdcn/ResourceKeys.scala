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
  val JvmMaxMemory     = "jvm.memory.max"     // 最大可用内存
  val JvmTotalMemory   = "jvm.memory.total"   // 已分配内存
  val JvmUsedMemory    = "jvm.memory.used"    // 已使用内存
  val JvmFreeMemory    = "jvm.memory.free"    // 空闲内存

  // === 系统物理内存 ===
  val SystemMemoryTotal = "system.memory.total"  // 总内存
  val SystemMemoryUsed  = "system.memory.used"   // 已使用
  val SystemMemoryFree  = "system.memory.free"   // 空闲
}
