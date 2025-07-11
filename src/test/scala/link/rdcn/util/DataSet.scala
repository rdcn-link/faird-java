package link.rdcn.struct

import link.rdcn.ConfigLoader
import org.apache.jena.rdf.model.Model
import org.apache.jena.vocabulary.RDF

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 14:17
 * @Modified By:
 */

case class DataFrameInfo(
                          name: String,
                          inputSource: InputSource,
                          schema: StructType
                        ) {
  def getSchemaUrl(url: String): String = url + name
}

case class DataSet(
                    dataSetName: String,
                    dataSetId: String,
                    dataFrames: List[DataFrameInfo]
                  ) {
  /** 生成 RDF 元数据模型 */
  def getMetadata(model: Model): Unit = {
    val datasetURI = s"dacp://${ConfigLoader.fairdConfig.hostName}:${ConfigLoader.fairdConfig.hostPort}/" + dataSetId
    val datasetRes = model.createResource(datasetURI)

    val hasFile = model.createProperty(datasetURI + "/hasFile")
    val hasName = model.createProperty(datasetURI + "/name")

    datasetRes.addProperty(RDF.`type`, model.createResource("DataSet"))
    datasetRes.addProperty(hasName, dataSetName)

    dataFrames.foreach { df =>
      datasetRes.addProperty(hasFile, df.name)
    }
  }

  def getDataFrameInfo(dataFrameName: String): Option[DataFrameInfo] = {
    dataFrames.find { dfInfo =>
      val normalizedDfPath: String = dfInfo.name.replace('\\', '/')
      normalizedDfPath.contains(dataFrameName)
    }
  }
}


