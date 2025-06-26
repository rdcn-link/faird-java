package link.rdcn;

import link.rdcn.provider.DataProvider;
import link.rdcn.server.FlightProducerImpl;
import link.rdcn.struct.*;
import link.rdcn.user.AuthProvider;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import scala.collection.JavaConverters;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/26 10:07
 * @Modified By:
 */
public class JProviderTest {


    public static void main(String[] args) throws IOException, InterruptedException {

        InputSource inputSource = new CSVSource(",", false);
        DataFrameInfo dfInfo = new DataFrameInfo("/home/dd/file_1.csv",inputSource, StructType.empty().add("id",  ValueTypeHelper.getIntType(), true)
                .add("name", ValueTypeHelper.getStringType(), true));
        java.util.List<DataFrameInfo> listD = new ArrayList<DataFrameInfo>();
        listD.add(dfInfo);
        DataSet ds1 = new DataSet("dd", "1", JavaConverters.asScalaBufferConverter(listD).asScala().toList());

        Location location = Location.forGrpcInsecure(ConfigBridge.getConfig().getHostPosition(), ConfigBridge.getConfig().getHostPort());
        BufferAllocator allocator = new RootAllocator();


        FlightProducerImpl producer = new FlightProducerImpl(allocator, location, new DataProvider(){

            @Override
            public AuthProvider authProvider() {
                return null;
            }

            @Override
            public List<DataSet> setDataSets() {
                java.util.List<DataSet> listD = new ArrayList<>();
                listD.add(ds1);
                return listD;
            }
        });
        FlightServer flightServer = FlightServer.builder(allocator, location, producer).build();
        flightServer.start();
        flightServer.awaitTermination();
    }

}
