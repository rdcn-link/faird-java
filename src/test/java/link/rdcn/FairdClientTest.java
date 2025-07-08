package link.rdcn;

import link.rdcn.client.FairdClient;
import link.rdcn.client.RemoteDataFrameImpl;
import link.rdcn.struct.Row;
import link.rdcn.user.UsernamePassword;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/4 09:46
 * @Modified By:
 */
public class FairdClientTest {

    public static <T> java.util.List<T> convert(Seq<T> scalaSeq) {
        return JavaConverters.seqAsJavaListConverter(scalaSeq).asJava();
    }

    @Test
    public void dataSetListTest(){
        FairdClient dc = FairdClient.connect("dacp://10.0.82.71:8232", new UsernamePassword("admin@instdb.cn", "admin001"));

        System.out.println("打印数据集列表--------------");
        java.util.List<String> dataSetNames = convert(dc.listDataSetNames());
        for (String name : dataSetNames) {
            System.out.println(name);
        }

        System.out.println("打印数据集 898cb373ebec4638af02d95fd564d7d6 所有数据帧名称--------------");
        java.util.List<String> frameNames = convert(dc.listDataFrameNames("898cb373ebec4638af02d95fd564d7d6"));
        for (String frameName : frameNames) {
            System.out.println(frameName);
        }

        System.out.println("打印数据集 898cb373ebec4638af02d95fd564d7d6 的元数据信息");
        String metaData = dc.getDataSetMetaData("898cb373ebec4638af02d95fd564d7d6");
        System.out.println(metaData);
    }

    @Test
    public void getExcelDataFrame(){
        FairdClient dc = FairdClient.connect("dacp://10.0.82.71:8232", new UsernamePassword("admin@instdb.cn", "admin001"));
        RemoteDataFrameImpl df = dc.open("64db30f117abe320a0cee7e5/Sheet1_vhtz.xlsx");
        //获取数据
        java.util.List<Row> rows = convert(df.limit(10).collect());

        for(Row row: rows){
            System.out.println(row);
        }
    }

    @Test
    public void getFileListDataFrame(){
        FairdClient dc = FairdClient.connect("dacp://10.0.82.71:8232", new UsernamePassword("admin@instdb.cn", "admin001"));
        RemoteDataFrameImpl df = dc.open("898cb373ebec4638af02d95fd564d7d6/高精度可调谐矩形光学滤波器");
        //获取数据 该数据帧可以看做一个非结构化数据的文件列表，每一行数据代表一个非结构化数据文件，本地跑vpn有限速会比较慢
        java.util.List<Row> rows = convert(df.limit(1).collect());

        for(Row row: rows){
            System.out.println(row);
        }
    }
}
