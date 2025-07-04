package link.rdcn.util

import link.rdcn.ConfigLoader
import link.rdcn.provider.{ArrowFlightDataStreamSource, DataProvider, DataStreamSource, DataStreamSourceFactory}
import link.rdcn.struct.{DataFrameInfo, DataSet, StructType}
import org.apache.jena.rdf.model.Model

import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:07
 * @Modified By:
 */

abstract class DataProviderImpl extends DataProvider{
   val dataSetsScalaList: List[DataSet]
   val dataFramePaths: (String => String)

  def listDataSetNames(): java.util.List[String] = {
    dataSetsScalaList.map(_.dataSetName).asJava
  }
  def getDataSetMetaData(dataSetName: String, rdfModel: Model): Unit = {
    val dataSet: DataSet = dataSetsScalaList.find(_.dataSetName == dataSetName).getOrElse(return rdfModel)
    dataSet.getMetadata(rdfModel)
  }
  def listDataFrameNames(dataSetName: String): java.util.List[String] = {
    val dataSet: DataSet = dataSetsScalaList.find(_.dataSetName == dataSetName).getOrElse(return new java.util.ArrayList)
    dataSet.dataFrames.map(_.name).asJava
  }
  def getDataFrameSource(dataFrameName: String): DataStreamSource = {
    val dataFrameInfo = getDataFrameInfo(dataFrameName).getOrElse(return ArrowFlightDataStreamSource(Iterator.empty, StructType.empty))
    DataStreamSourceFactory.getDataFrameSourceFromInputSource(dataFrameName, dataFrameInfo.schema, dataFrameInfo.inputSource)
  }

  def getDataFrameSchema(dataFrameName: String): StructType = {
    getDataFrameInfo(dataFrameName).map(_.schema).getOrElse(StructType.empty)
  }

  def getDataFrameSchemaURL(dataFrameName: String): String = {
    getDataFrameInfo(dataFrameName).map(_.getSchemaUrl(s"dacp://${ConfigLoader.fairdConfig.getHostName}:${ConfigLoader.fairdConfig.getHostPort}")).getOrElse("")
  }

  private def getDataFrameInfo(dataFrameName: String): Option[DataFrameInfo] = {
    dataSetsScalaList.foreach(ds => {
      val dfInfo = ds.getDataFrameInfo(dataFrameName)
      if(dfInfo.nonEmpty) return dfInfo
    })
    None
  }

}




