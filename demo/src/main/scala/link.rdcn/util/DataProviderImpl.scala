package link.rdcn.util

import link.rdcn.ConfigLoader
import link.rdcn.provider.{DataFrameDocument, DataProvider, DataStreamSource, DataStreamSourceFactory}
import link.rdcn.struct.{CSVSource, DataFrameInfo, DataSet, DirectorySource, InputSource, Row, StructType}
import org.apache.jena.rdf.model.Model

import java.io.File
import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:07
 * @Modified By:
 */

abstract class DataProviderImpl extends DataProvider {
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

  def getDataStreamSource(dataFrameName: String): DataStreamSource = {
    val dataFrameInfo: DataFrameInfo = getDataFrameInfo(dataFrameName).getOrElse(return new DataStreamSource {
      override def rowCount: Long = -1

      override def schema: StructType = StructType.empty

      override def iterator: Iterator[Row] = Iterator.empty
    })
    dataFrameInfo.inputSource match {
      case _: CSVSource => DataStreamSourceFactory.createCsvDataStreamSource(new File(dataFrameInfo.name))
      case _: DirectorySource => DataStreamSourceFactory.createFileListDataStreamSource(new File(dataFrameInfo.name))
      case _: InputSource => ???
    }

  }

  override def getDataFrameDocument(dataFrameName: String): DataFrameDocument = {
    val dataFrameInfo: DataFrameInfo = getDataFrameInfo(dataFrameName).getOrElse(return null)
    new DataFrameDocument {
      override def getSchemaURL(): Option[String] = {
        getDataFrameInfo(dataFrameName).map(_.getSchemaUrl(s"dacp://${ConfigLoader.fairdConfig.hostName}:${ConfigLoader.fairdConfig.hostPort}"))
      }

      override def getColumnURL(colName: String): Option[String] = Some("")

      override def getColumnAlias(colName: String): Option[String] = Some("")

      override def getColumnTitle(colName: String): Option[String] = Some("")
    }
  }

  def getDataFrameSchema(dataFrameName: String): StructType = {
    getDataFrameInfo(dataFrameName).map(_.schema).getOrElse(StructType.empty)
  }

  def getDataFrameSchemaURL(dataFrameName: String): String = {
    getDataFrameInfo(dataFrameName).map(_.getSchemaUrl(s"dacp://${ConfigLoader.fairdConfig.hostName}:${ConfigLoader.fairdConfig.hostPort}")).getOrElse("")
  }

  private def getDataFrameInfo(dataFrameName: String): Option[DataFrameInfo] = {
    dataSetsScalaList.foreach(ds => {
      val dfInfo = ds.getDataFrameInfo(dataFrameName)
      if (dfInfo.nonEmpty) return dfInfo
    })
    None
  }

}




