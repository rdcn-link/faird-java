package link.rdcn.provider;

import link.rdcn.struct.StructType;

import java.util.List;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/27 18:13
 * @Modified By:
 */
public interface DataProvider {

    List<String> listDataSetNames();

    void getDataSetMetaData(String dataSetName, org.apache.jena.rdf.model.Model rdfModel);

    List<String> listDataFrameNames(String dataSetName);

    DataStreamSource getDataFrameSource(String dataFrameName);

    StructType getDataFrameSchema(String dataFrameName);

    String getDataFrameSchemaURL(String dataFrameName);
}
