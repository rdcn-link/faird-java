package link.rdcn.provider

import link.rdcn.struct.{DataFrameInfo, DataSet, StructType}
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:07
 * @Modified By:
 */

trait DataProvider {
  val dataSets: List[DataSet]

  def listDataSetNames(): List[String] = {
    dataSets.map(_.dataSetName)
  }
  def getDataSetMetaData(dataSetName: String, rdfModel: Model): Unit = {
    val dataSet: DataSet = dataSets.find(_.dataSetName == dataSetName).getOrElse(return rdfModel)
    dataSet.getMetadata(rdfModel)
  }
  def listDataFrameNames(dataSetName: String): List[String] = {
    val dataSet: DataSet = dataSets.find(_.dataSetName == dataSetName).getOrElse(return List.empty)
    dataSet.dataFrames.map(_.name)
  }
  def getDataFrameSource(dataFrameName: String, factory: DataStreamSourceFactory): DataStreamSource = {
    val dataFrameInfo = getDataFrameInfo(dataFrameName).getOrElse(return ArrowFlightDataStreamSource(Iterator.empty, StructType.empty))
    factory.createDataFrameSource(dataFrameInfo)
  }

  def getDataFrameInfo(dataFrameName: String): Option[DataFrameInfo] = {
    dataSets.foreach(ds => {
      val dfInfo = ds.getDataFrameInfo(dataFrameName)
      if(dfInfo.nonEmpty) return dfInfo
    })
    None
  }
}




