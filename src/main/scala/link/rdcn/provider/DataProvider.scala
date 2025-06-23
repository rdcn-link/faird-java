package link.rdcn.provider

import link.rdcn.Logging
import link.rdcn.client.RemoteDataFrame
import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.apache.spark.sql.types.{BinaryType, DataType, StringType, StructType}

import java.io.StringWriter
import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:07
 * @Modified By:
 */

trait DataProvider {
  def checkPermission(dataFrameName: String, userId: String, operation: String): Boolean
  def listDataSetNames(): List[String]
  def getRDFModel(dataFrameName: String): Model
  def listDataFrameNames(dataSetName: String): List[String]
  def getDataFrameSource(remoteDataFrame: RemoteDataFrame, factory: DynamicDataFrameSourceFactory): DataFrameSource
  def mockDataSetMetaData(): Map[String, Model]
  def getSchema(dataFrameName: String): StructType
  def getMetaData(dataFrameName: String): String
  def getSchemaURI(dataFrameName: String): String
  def getPath(remoteDataFrame: RemoteDataFrame):String
}

case class MetaDataSet()
//provider.listDataSetNames(): List[String]
//provider.getDataSetMetaData(dataSetName, rdfModel): Unit //RDF形式的元数据
//provider.listDataFrameNames(dataSetName): List[String]
//provider.getDataFrameSource(dataFrameName, MetaDataSet,dataFrameSourceFactory): DataFrameSource



