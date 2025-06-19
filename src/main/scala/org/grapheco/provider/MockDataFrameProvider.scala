package org.grapheco.provider

import org.apache.jena.rdf.model.{Model, ModelFactory, ResourceFactory}
import org.grapheco.Logging

import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:07
 * @Modified By:
 */

class MockDataFrameProvider extends DataFrameProvider with Logging{

  private val dataSets = Map(
    "climate" -> List("climate_temp", "climate_rain"),
    "population" -> List("pop_urban", "pop_rural")
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

  override def getDataSetMetaData(dataSetName: String, rdfModel: Model): Unit = {
    val ns = "http://example.org/dataset/"
    val resource = rdfModel.createResource(ns + dataSetName)
    rdfModel.add(resource, rdfModel.createProperty(ns + "type"), "DataSet")
    rdfModel.add(resource, rdfModel.createProperty(ns + "name"), dataSetName)
    rdfModel.add(resource, rdfModel.createProperty(ns + "contains"),
      dataSets.getOrElse(dataSetName, Nil).mkString(", "))
  }

  override def listDataFrameNames(dataSetName: String): List[String] = {
    dataSets.getOrElse(dataSetName, Nil)
  }

  override def getDataFrameSource(dataFrameName: String, factory: DataFrameSourceFactory): DataFrameSource = {
    // For demonstration, assume all files are in "/mock/data"
//    factory.createFileListDataFrameSource(s"/Users/renhao/Downloads", dataFrameName)
    factory.createFileListDataFrameSource(s"C:\\Users\\Yomi\\Downloads\\数据\\others", dataFrameName)
  }
}

