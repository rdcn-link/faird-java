package link.rdcn.struct

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/27 18:56
 * @Modified By:
 */
sealed trait InputSource

case class CSVSource(
                      delimiter: String = ",",
                      head: Boolean = false
                    ) extends InputSource

case class JSONSource(
                       multiline: Boolean = false
                     ) extends InputSource

case class DirectorySource(
                            recursive: Boolean = true
                          ) extends InputSource

case class StructuredSource() extends InputSource