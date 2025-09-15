package link.rdcn.server.dftp

import link.rdcn.struct.{DefaultDataFrame, Row, StructType}
import link.rdcn.util.ClosableIterator
import org.apache.arrow.vector.VectorSchemaRoot

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/26 19:56
 * @Modified By:
 */
class DataFrameWithArrowRoot(root: VectorSchemaRoot, override val schema: StructType, iter: Iterator[Row]) extends DefaultDataFrame(schema, ClosableIterator(iter)())