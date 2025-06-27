package link.rdcn;

import link.rdcn.provider.DataProvider;
import link.rdcn.provider.DataProviderImplByDataSetList;
import link.rdcn.provider.DataStreamSource;
import link.rdcn.provider.DataStreamSourceFactory;
import link.rdcn.server.FlightProducerImpl;
import link.rdcn.struct.*;
import link.rdcn.user.AuthProvider;
import link.rdcn.user.AuthenticatedUser;
import link.rdcn.user.Credentials;
import link.rdcn.user.exception.AuthException;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;
import scala.collection.JavaConverters;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/26 10:07
 * @Modified By:
 */
public class JProviderTest {

    public static void m1(String[] args) {
        Model rdfModel = ModelFactory.createDefaultModel();
        String hostname = ConfigBridge.getConfig().getHostName(); //配置文件中 faird.hostName=cerndc
        int port = ConfigBridge.getConfig().getHostPort(); //faird.hostPort=3101
        String dataSetID = "根据dataSetName拿ID";
        String datasetURI = "dacp://" + hostname + ":" + port + "/" + dataSetID;
        Resource datasetRes = rdfModel.createResource(datasetURI);

        Property hasFile = rdfModel.createProperty(datasetURI + "/hasFile");
        Property hasName = rdfModel.createProperty(datasetURI + "/name");

        datasetRes.addProperty(RDF.type, rdfModel.createResource("DataSet"));
        datasetRes.addProperty(hasName, "dataSetName");
        List<String> dataFrameNames = new java.util.ArrayList<>();
        dataFrameNames.add("123123");
        for (String dataFrameName : dataFrameNames) {
            datasetRes.addProperty(hasFile, dataFrameName);
        }
        System.out.println(rdfModel.toString());
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        InputSource inputSource = new CSVSource(",", false);
        DataFrameInfo dfInfo = new DataFrameInfo("/home/dd/file_1.csv",inputSource, StructType.empty().add("id",  ValueTypeHelper.getIntType(), true)
                .add("name", ValueTypeHelper.getStringType(), true));
        java.util.List<DataFrameInfo> listD = new ArrayList<DataFrameInfo>();
        listD.add(dfInfo);
        DataSet ds1 = new DataSet("dd", "1", JavaConverters.asScalaBufferConverter(listD).asScala().toList());

        Location location = Location.forGrpcInsecure(ConfigBridge.getConfig().getHostPosition(), ConfigBridge.getConfig().getHostPort());
        BufferAllocator allocator = new RootAllocator();

        AuthProvider au = new AuthProvider(){
            @Override
            public AuthenticatedUser authenticate(Credentials credentials) throws AuthException {
                return new AuthenticatedUser("1", new java.util.HashSet<String>(),new java.util.HashSet<String>());
            }
            @Override
            public boolean authorize(AuthenticatedUser user, String dataFrameName) {
                return true;
            }
        };

        DataProvider dataProvider = new DataProvider(){


            @Override
            public List<String> listDataSetNames() {
                return Collections.emptyList();
            }

            @Override
            public void getDataSetMetaData(String dataSetName, Model rdfModel) {
                String hostname = ConfigBridge.getConfig().getHostName(); //配置文件中 faird.hostName=cerndc
                int port = ConfigBridge.getConfig().getHostPort(); //faird.hostPort=3101
                String dataSetID = "根据dataSetName拿ID";
                String datasetURI = "dacp://" + hostname + ":" + port + "/" + dataSetID;
                Resource datasetRes = rdfModel.createResource(datasetURI);

                Property hasFile = rdfModel.createProperty(datasetURI + "/hasFile");
                Property hasName = rdfModel.createProperty(datasetURI + "/name");

                datasetRes.addProperty(RDF.type, rdfModel.createResource("DataSet"));
                datasetRes.addProperty(hasName, dataSetName);
                List<String> dataFrameNames = new java.util.ArrayList<>();
                dataFrameNames.add("123123");
                dataFrameNames.add("dsfsdf");
                for (String dataFrameName : dataFrameNames) {
                    datasetRes.addProperty(hasFile, dataFrameName);
                }
            }

            @Override
            public List<String> listDataFrameNames(String dataSetName) {
                return Collections.emptyList();
            }

            @Override
            public DataStreamSource getDataFrameSource(String dataFrameName) {
                if(dataFrameName == "/mnt/sdb/csv/a.csv"){
                    return null;
                }else {
                    DirectorySource directorySource = new DirectorySource(false);
                    return DataStreamSourceFactory.getDataFrameSourceFromInputSource(dataFrameName, getDataFrameSchema(dataFrameName), directorySource);
                }
//                StructuredSource source = new StructuredSource();
//                return DataStreamSourceFactory.getDataFrameSourceFromInputSource(dataFrameName, getDataFrameSchema(dataFrameName), source);

            }

            @Override
            public StructType getDataFrameSchema(String dataFrameName) {
                return StructType.empty().add("id", ValueTypeHelper.getIntType(), true)
                        .add("name", ValueTypeHelper.getStringType(), true)
                        .add("fileType", ValueTypeHelper.getStringType(), true)
                        .add("size", ValueTypeHelper.getLongType(), true)
                        .add("createDate", ValueTypeHelper.getLongType(), true)
                        .add("lastModifyDate", ValueTypeHelper.getLongType(), true)
                        .add("lastAccessDate", ValueTypeHelper.getLongType(), true)
                        .add("fileStream", ValueTypeHelper.getBinaryType(), true);
            }

            @Override
            public String getDataFrameSchemaURL(String dataFrameName) {
                String hostname = ConfigBridge.getConfig().getHostName(); //配置文件中 faird.hostName=cerndc
                int port = ConfigBridge.getConfig().getHostPort(); //faird.hostPort=3101
                String dataFrameSchemaURL = "dacp://" + hostname + ":" + port + "/" + dataFrameName;
                return dataFrameSchemaURL;
            }
        };

        FlightProducerImpl producer = new FlightProducerImpl(allocator, location, dataProvider, au );

        try {
            System.out.println(io.netty.buffer.PooledByteBufAllocator.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation());
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            System.out.println(cfjd.io.netty.buffer.PooledByteBufAllocator.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation());
        } catch (Exception e) {
            e.printStackTrace();
        }

        FlightServer flightServer = FlightServer.builder(allocator, location, producer).build();
        flightServer.start();
        System.out.println("启动666");
        flightServer.awaitTermination();
    }

}
