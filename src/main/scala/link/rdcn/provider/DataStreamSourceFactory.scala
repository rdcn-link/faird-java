package link.rdcn.provider

import link.rdcn.Logging
import link.rdcn.struct.{CSVSource, DataFrameInfo, DirectorySource, InputSource, Row, StructType, StructuredSource, ValueTypeHelper}
import link.rdcn.util.DataUtils

import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/23 10:14
 * @Modified By:
 */

object DataStreamSourceFactory{
  def getDataFrameSourceFromInputSource(dataFrameName: String, schema: StructType, inputSource: InputSource): DataStreamSource = {
    val stream: Iterator[Row] = inputSource match {
      case CSVSource(delimiter, head) =>
        val iterLines = DataUtils.getFileLines(dataFrameName)
          .map(line => Row(line.split(delimiter): _*))
        if(head) iterLines.next()
        iterLines.map(DataUtils.convertStringRowToTypedRow(_, schema))
      case StructuredSource() => DataUtils.getFileLines(dataFrameName).map(line => Row(line))
      case DirectorySource(false) =>
        DataUtils.listFilesWithAttributes(dataFrameName).toIterator.zipWithIndex
          // schema [ID, name, size, 文件类型, 创建时间, 最后修改时间, 最后访问时间, file]
          .map{case (file, index) => (file._1.getName, file._2.size(), DataUtils.getFileTypeByExtension(file._1), file._2.creationTime().toMillis, file._2.lastModifiedTime().toMillis, file._2.lastAccessTime().toMillis,file._1)}
          .map(Row.fromTuple(_))
    }
    ArrowFlightDataStreamSource(stream, schema)
  }

  def getDataFrameSourceFromJavaList(stream: java.util.Iterator[java.util.List[Object]], schema: StructType): DataStreamSource = {
    val rows = stream.asScala.map(_.asScala).map(Row.fromSeq(_))
    ArrowFlightDataStreamSource(rows, schema)
  }
}