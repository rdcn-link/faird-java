/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 16:02
 * @Modified By:
 */
package link.rdcn;

import link.rdcn.client.DataFrame;
import link.rdcn.client.FairdClient;
import link.rdcn.client.RemoteDataFrame;
import link.rdcn.client.dag.TransformerDAG;
import link.rdcn.user.Credentials;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;

public class JFairdClient {
    private FairdClient fairdClient;

    public JFairdClient(FairdClient fairdClient) {
        this.fairdClient = fairdClient;
    }

    public RemoteDataFrame open(String dataFrameName) {
        return fairdClient.open(dataFrameName);
    }

    public List<String> listDataSetNames() {
        return convertToJavaList(fairdClient.listDataSetNames());
    }

    public List<String> listDataFrameNames(String dsName) {
        return convertToJavaList(fairdClient.listDataFrameNames(dsName));
    }

    public String getDataSetMetaData(String dsName) {
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

    public List<DataFrame> execute(TransformerDAG transformerDAG) {
        return convertToJavaList(fairdClient.execute(transformerDAG));
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
