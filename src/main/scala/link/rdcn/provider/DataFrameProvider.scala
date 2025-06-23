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

trait DataFrameProvider {
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

class MockDataFrameProvider extends DataFrameProvider with Logging{

  private val ns = "http://example.org/dataset/"

  private val dataSets = Map(
    "climate" -> List("climate_temp", "climate_rain"),
    "population" -> List("pop_urban", "pop_rural"),
    "data" -> List("mp4"),
  )

  private val dataSetsPath = Map(
    "dacp://10.0.0.1/bindata" -> "C:\\Users\\NatsusakiYomi\\Downloads\\数据\\mp4"
  )

  private val permissions = mutable.Set(
    ("climate_temp", "user1", "read"),
    ("pop_urban", "user1", "read"),
    ("pop_urban", "user1", "write")
  )

  override def checkPermission(dataFrameName: String, userId: String, operation: String): Boolean = {
    permissions.contains((dataFrameName, userId, operation))
  }

  override def listDataSetNames(): List[String] = dataSets.keys.toList

  def mockDataSetMetaData(): Map[String, Model] = ???



  override def getRDFModel(dataFrameName: String): Model = {

    val rdfModel = ModelFactory.createDefaultModel()
    val resource = rdfModel.createResource(ns + dataFrameName)
    rdfModel.add(resource, rdfModel.createProperty(ns + "type"), "DataSet")
    rdfModel.add(resource, rdfModel.createProperty(ns + "title"), dataFrameName)
    rdfModel.add(resource, rdfModel.createProperty(ns + "description"), "description")
    rdfModel.add(resource, rdfModel.createProperty(ns + "lastModified"), "2025-6-21")
    rdfModel.add(resource, rdfModel.createProperty(ns + "dataType"), "File")
    rdfModel.add(resource, rdfModel.createProperty(ns + "dataFormat"), "bin")
    rdfModel.add(resource, rdfModel.createProperty(ns + "size"), "100MB")
    rdfModel.add(resource, rdfModel.createProperty(ns + "contains"),
    dataSets.getOrElse(dataFrameName, Nil).mkString(", "))
    val structType =
            new StructType()
              .add("name", StringType)
              .add("path", StringType)
              .add("ext", StringType)
              .add("type", StringType)
              .add("size", StringType)
              .add("lastModiefied", StringType)
              .add("bin", BinaryType)
    rdfModel.add(resource, rdfModel.createProperty(ns + "schema"),
      structType.json)

    rdfModel
  }

  override def listDataFrameNames(dataSetName: String): List[String] = {
    dataSets.getOrElse(dataSetName, Nil)
  }

  override def getSchema(dataFrameName: String): StructType = {
    val model = getRDFModel(dataFrameName)
    val subject = model.getResource(ns+dataFrameName)
    val schemaJson = subject.getProperty(model.getProperty(ns+"schema"))
      .getString
    DataType.fromJson(schemaJson).asInstanceOf[StructType]

  }

  override def getMetaData(dataFrameName: String): String = {
    val model = getRDFModel(dataFrameName)
    val sw = new StringWriter()
    model.write(sw,"TURTLE")
    sw.toString.asInstanceOf[String]
      }


  override def getDataFrameSource(remoteDataFrame: RemoteDataFrame, factory: DynamicDataFrameSourceFactory): DataFrameSource = {
    // For demonstration, assume all files are in "/mock/data"
    val dataSetName = remoteDataFrame.source.datasetId
    remoteDataFrame.setRDFModel(getRDFModel(dataSetName))
    remoteDataFrame.setSchema(getSchema(dataSetName).toString())
    remoteDataFrame.setMetaData(getMetaData(dataSetName))
    remoteDataFrame.setSchemaURI("http://rdcn.link/schema/"+dataSetName)
    remoteDataFrame.setPropertiesMap
//    log.info("getting DataFrameSource...")
    factory.createFileListDataFrameSource(remoteDataFrame)
  }

  override def getPath(remoteDataFrame: RemoteDataFrame): String = {
    dataSetsPath.getOrElse(remoteDataFrame.source.datasetId,"")
  }

  override def getSchemaURI(dataFrameName: String): String = {
    "http://rdcn.link/schema/"+dataFrameName
  }
}

