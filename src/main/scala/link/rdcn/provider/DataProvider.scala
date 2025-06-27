package link.rdcn.provider

import link.rdcn.struct.{DataFrameInfo, DataSet, StructType}
import link.rdcn.user.AuthProvider
import org.apache.jena.rdf.model.Model

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:07
 * @Modified By:
 */

abstract class DataProvider {
  def setDataSets(): java.util.List[DataSet]
  val authProvider: AuthProvider

  def listDataSetNames(): List[String] = {
    dataSetsScalaList.map(_.dataSetName)
  }
  def getDataSetMetaData(dataSetName: String, rdfModel: Model): Unit = {
    val dataSet: DataSet = dataSetsScalaList.find(_.dataSetName == dataSetName).getOrElse(return rdfModel)
    dataSet.getMetadata(rdfModel)
  }
  def listDataFrameNames(dataSetName: String): List[String] = {
    val dataSet: DataSet = dataSetsScalaList.find(_.dataSetName == dataSetName).getOrElse(return List.empty)
    dataSet.dataFrames.map(_.name)
  }
  def getDataFrameSource(dataFrameName: String, factory: DataStreamSourceFactory): DataStreamSource = {
    val dataFrameInfo = getDataFrameInfo(dataFrameName).getOrElse(return ArrowFlightDataStreamSource(Iterator.empty, StructType.empty))
    factory.createDataFrameSource(dataFrameInfo)
  }

  def getDataFrameInfo(dataFrameName: String): Option[DataFrameInfo] = {
    dataSetsScalaList.foreach(ds => {
      val dfInfo = ds.getDataFrameInfo(dataFrameName)
      if(dfInfo.nonEmpty) return dfInfo
    })
    None
  }

  private val dataSetsScalaList = setDataSets.asScala.toList

}




