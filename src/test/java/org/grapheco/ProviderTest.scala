package org.grapheco

import org.apache.jena.rdf.model.ModelFactory
import org.grapheco.provider.{MockDataFrameProvider, SimpleDataFrameSourceFactory}
import org.junit.jupiter.api.Test

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/16 18:08
 * @Modified By:
 */
class ProviderTest {

  @Test
  def m1(): Unit = {
    val provider = new MockDataFrameProvider
    val factory = new SimpleDataFrameSourceFactory

    println(provider.checkPermission("pop_urban", "user1", "read")) // true
    println(provider.listDataSetNames()) // List(climate, population)
    println(provider.listDataFrameNames("climate")) // List(climate_temp, climate_rain)

    val model = ModelFactory.createDefaultModel()
    provider.getDataSetMetaData("climate", model)
    model.write(System.out, "TURTLE")

    val source = provider.getDataFrameSource("part-00000", factory)
    println(s"Source URI: ${source.sourceUri}")
  }
}
