package link.rdcn

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 17:23
 * @Modified By:
 */
case class FairdConfig(
                        hostName: String,
                        hostTitle: String,
                        hostPosition: String,
                        hostDomain: String,
                        hostPort: Int
                      )

object FairdConfig {

  /** 从 java.util.Properties 加载配置生成 FairdConfig 实例 */
  def load(props: java.util.Properties): FairdConfig = {
    def get(key: String): String =
      Option(props.getProperty(key))
        .getOrElse(throw new IllegalArgumentException(s"Missing config key: $key"))

    FairdConfig(
      hostName = get("host.name"),
      hostTitle = get("host.title"),
      hostPosition = get("host.position"),
      hostDomain = get("host.domain"),
      hostPort = get("host.port").toInt,
    )
  }
}