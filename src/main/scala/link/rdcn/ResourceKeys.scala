package link.rdcn

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/16 14:13
 * @Modified By:
 */
object ResourceKeys {
  // === CPU 资源 ===
  val CPU_CORES        = "cpu.cores"           // CPU核心数
  val CPU_USAGE_PERCENT = "cpu.usage.percent"   // CPU使用率 (%)

  // === JVM 内存 ===
  val JVM_MEMORY_MAX     = "jvm.memory.max"     // 最大可用内存
  val JVM_TOTAL_MEMORY   = "jvm.memory.total"   // 已分配内存
  val JVM_USED_MEMORY    = "jvm.memory.used"    // 已使用内存
  val JVM_FREE_MEMORY    = "jvm.memory.free"    // 空闲内存

  // === 系统物理内存 ===
  val SYSTEM_TOTAL_MEMORY = "system.memory.total"  // 总内存
  val SYSTEM_USE_MEMORY  = "system.memory.used"   // 已使用
  val SYSTEM_FREE_MEMORY  = "system.memory.free"   // 空闲
}
