package link.rdcn;

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 16:02
 * @Modified By:
 */


import link.rdcn.client.dacp.DacpClient;
import link.rdcn.client.recipe.Flow;
import link.rdcn.struct.DataFrame;
import link.rdcn.struct.ExecutionResult;
import link.rdcn.user.Credentials;
import org.apache.jena.rdf.model.Model;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.File;
import java.util.List;
import java.util.Map;

public class JFairdClient {
    private DacpClient dacpClient;

    public JFairdClient(DacpClient dacpClient) {
        this.dacpClient = dacpClient;
    }

    public DataFrame get(String dataFrameName) {
        return dacpClient.get(dataFrameName);
    }

    public List<String> listDataSetNames() {
        return convertToJavaList(dacpClient.listDataSetNames());
    }

    public List<String> listDataFrameNames(String dsName) {
        return convertToJavaList(dacpClient.listDataFrameNames(dsName));
    }

    public Model getDataSetMetaData(String dsName) {
        return dacpClient.getDataSetMetaData(dsName);
    }

    public Map<String, String> getHostInfo() {
        return JavaConverters.mapAsJavaMap(dacpClient.getHostInfo());
    }

    public Map<String, String> getServerResourceInfo() {
        return JavaConverters.mapAsJavaMap(dacpClient.getServerResourceInfo());
    }

    public void close() {
        dacpClient.close();
    }

    public JExecutionResult execute(Flow flow) {
        ExecutionResult executionResult = dacpClient.execute(flow);
        return
                new JExecutionResult() {

                    @Override
                    public DataFrame single() {
                        return executionResult.single();
                    }

                    @Override
                    public DataFrame get(String name) {
                        return executionResult.get(name);
                    }

                    @Override
                    public Map<String, DataFrame> map() {
                        return convertToJavaMap(executionResult.map());
                    }
                };
    }


    public static JFairdClient connect(String url, Credentials credentials) {
        return new JFairdClient(DacpClient.connect(url, credentials));
    }

    public static JFairdClient connectTLS(String url, Credentials credentials) {
        return new JFairdClient(DacpClient.connectTLS(url, new File("C:/Users/ASUS/Documents/Projects/PycharmProjects/Faird/Faird/faird-core/target/test-classes/tls/faird"), credentials));
    }

    public static <T> java.util.List<T> convertToJavaList(Seq<T> scalaSeq) {
        return JavaConverters.seqAsJavaListConverter(scalaSeq).asJava();
    }

    public static <K, V> java.util.Map<K, V> convertToJavaMap(scala.collection.Map<K, V> scalaMap) {
        return JavaConverters.mapAsJavaMap(scalaMap);
    }


}

interface JExecutionResult {
    DataFrame single();

    DataFrame get(String name);

    Map<String, DataFrame> map();
}
