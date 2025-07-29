package link.rdcn;

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 16:02
 * @Modified By:
 */


import link.rdcn.client.FairdClient;
import link.rdcn.client.RemoteDataFrame;
import link.rdcn.client.dag.Flow;
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

    public RemoteDataFrame get(String dataFrameName) {
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

    public List<link.rdcn.struct.DataFrame> execute(Flow flow) {
        return convertToJavaList(fairdClient.execute(flow));
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

}
