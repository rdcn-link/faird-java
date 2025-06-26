package link.rdcn.provider

import link.rdcn.Logging
import link.rdcn.struct.{CSVSource, DataFrameInfo, DirectorySource, InputSource, Row, StructType, StructuredSource}
import link.rdcn.util.DataUtils

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/23 10:14
 * @Modified By:
 */
trait DataStreamSourceFactory {
  def createDataFrameSource(dataFrameInfo: DataFrameInfo): DataStreamSource
}

class DynamicDataStreamSourceFactory() extends DataStreamSourceFactory with Logging {

  override def createDataFrameSource(dataFrameInfo: DataFrameInfo): DataStreamSource = {
    val inputSource = dataFrameInfo.inputSource
    val stream: Iterator[Row] = inputSource match {
      case CSVSource(delimiter, head) =>
        val iterLines = DataUtils.getFileLines(dataFrameInfo.name)
        .map(line => Row(line.split(delimiter): _*))
        if(head) iterLines.next()
        iterLines.map(DataUtils.convertStringRowToTypedRow(_, dataFrameInfo.schema))
      case StructuredSource() => DataUtils.getFileLines(dataFrameInfo.name).map(line => Row(line))
      case DirectorySource(false) =>
        val chunkSize: Int = 5 * 1024 * 1024
        DataUtils.listFilesWithAttributes(dataFrameInfo.name).toIterator.zipWithIndex
          // schema [ID, name, size, 文件类型, 创建时间, 最后修改时间, 最后访问时间, file]
          .map{case (file, index) => (file._1.getName, file._2.size(), DataUtils.getFileTypeByExtension(file._1), file._2.creationTime().toMillis, file._2.lastModifiedTime().toMillis, file._2.lastAccessTime().toMillis,file._1)}
          .map(Row.fromTuple(_))
    }

    ArrowFlightDataStreamSource(stream, dataFrameInfo.schema)
  }

}