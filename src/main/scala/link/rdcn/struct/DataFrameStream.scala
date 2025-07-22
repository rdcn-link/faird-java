package link.rdcn.struct

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 15:51
 * @Modified By:
 */
case class DataFrameStream(
                      schema: StructType,
                      stream: Iterator[Row]
                    ) {

}
