package link.rdcn;

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 16:02
 * @Modified By:
 */


import link.rdcn.client.dacp.FairdClient;
import link.rdcn.client.RemoteDataFrameProxy;
import link.rdcn.client.dag.Flow;
import link.rdcn.struct.DataFrame;
import link.rdcn.struct.ExecutionResult;
import link.rdcn.user.Credentials;
import org.apache.jena.rdf.model.Model;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;

public class JFairdClient {
    private FairdClient fairdClient;

    public JFairdClient(FairdClient fairdClient) {
        this.fairdClient = fairdClient;
    }

    public RemoteDataFrameProxy get(String dataFrameName) {
        return fairdClient.get(dataFrameName);
    }

    public List<String> listDataSetNames() {
        return convertToJavaList(fairdClient.listDataSetNames());
    }

    public List<String> listDataFrameNames(String dsName) {
        return convertToJavaList(fairdClient.listDataFrameNames(dsName));
    }

    public Model getDataSetMetaData(String dsName) {
        return fairdClient.getDataSetMetaData(dsName);
    }

    public Map<String, String> getHostInfo() {
        return JavaConverters.mapAsJavaMap(fairdClient.getHostInfo());
    }

    public Map<String, String> getServerResourceInfo() {
        return JavaConverters.mapAsJavaMap(fairdClient.getServerResourceInfo());
    }

    public void close() {
        fairdClient.close();
    }

    public JExecutionResult execute(Flow flow) {
        ExecutionResult executionResult =  fairdClient.execute(flow);
        return
        new JExecutionResult(){

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
        return new JFairdClient(FairdClient.connect(url, credentials));
    }

    public static JFairdClient connectTLS(String url, Credentials credentials) {
        return new JFairdClient(FairdClient.connectTLS(url, credentials));
    }

    public static <T> java.util.List<T> convertToJavaList(Seq<T> scalaSeq) {
        return JavaConverters.seqAsJavaListConverter(scalaSeq).asJava();
    }

    public static <K,V> java.util.Map<K,V> convertToJavaMap(scala.collection.Map<K,V> scalaMap) {
        return JavaConverters.mapAsJavaMap(scalaMap);
    }


}

interface JExecutionResult {
    DataFrame single();
    DataFrame get(String name);
    Map<String, DataFrame> map();
}
