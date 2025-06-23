package link.rdcn.provider

import link.rdcn.Logging
import link.rdcn.client.RemoteDataFrame

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/23 10:14
 * @Modified By:
 */
trait DataFrameSourceFactory {
  def createFileListDataFrameSource(remoteDataFrame: RemoteDataFrame): DataFrameSource
}

class DynamicDataFrameSourceFactory(provider: DataProvider) extends DataFrameSourceFactory with Logging {


  override def createFileListDataFrameSource(remoteDataFrame: RemoteDataFrame): DataFrameSource = {
    val propertiesMap: Map[String, String] = remoteDataFrame.getPropertiesMap

    val dataFormat: String = propertiesMap("dataFormat")
    dataFormat match {
      case "csv" => new CSVSource(remoteDataFrame,provider)
      case "structured" => new StructuredSource(remoteDataFrame,provider)
      case _ => new DirectorySource(remoteDataFrame,provider)
      //      case other => throw new IllegalArgumentException(s"Unsupported format: $other")
    }
  }
}
