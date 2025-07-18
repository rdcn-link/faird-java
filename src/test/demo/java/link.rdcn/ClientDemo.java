package link.rdcn;

import link.rdcn.client.*;
import link.rdcn.client.dag.DAGNode;
import link.rdcn.client.dag.SourceNode;
import link.rdcn.client.dag.TransformerDAG;
import link.rdcn.client.dag.UDFFunction;
import link.rdcn.provider.DataFrameDocument;
import link.rdcn.struct.Row;
import link.rdcn.user.UsernamePassword;
import scala.Function1;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;

import static link.rdcn.TestFairdConfigKeys.*;

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/14 19:38
 * @Modified By:
 */
public class ClientDemo {

    public static void main(String[] args) {
        // 通过用户名密码非加密连接FairdClient
//        FairdClient dc = FairdClient.connect("dacp://localhost:3101", new UsernamePassword("admin@instdb.cn", "admin001"));
        // 通过用户名密码tls加密连接FairdClient 需要用户端进行相关配置
        // 客户端证书路径配置
        System.setProperty("javax.net.ssl.trustStore", Paths.get(System.getProperty("user.dir"),"target/test-classes/tls/faird").toString());
       FairdClient dc = FairdClient.connectTLS("dacp://localhost:3101", new UsernamePassword("admin@instdb.cn", "admin001"));
        // 匿名连接FairdClient
//        FairdClient dcAnonymous = FairdClient.connect("dacp://localhost:3101", Credentials.ANONYMOUS());

        //获得所有的数据集名称
        System.out.println("--------------打印数据集列表--------------");
        List<String> dataSetNames = convertToJavaList(dc.listDataSetNames());
        for (String name : dataSetNames) {
            System.out.println(name);
        }

        //获得指定数据集的所有的数据帧名称
        System.out.println("--------------打印数据集 csv 所有数据帧名称--------------");
        List<String> frameNames = convertToJavaList(dc.listDataFrameNames("csv"));
        for (String frameName : frameNames) {
            System.out.println(frameName);
        }

        //获得指定数据集的元数据信息
        System.out.println("--------------打印数据集 csv 的元数据信息--------------");
        String metaData = dc.getDataSetMetaData("csv");
        System.out.println(metaData);

        //获得host基本信息
        System.out.println("--------------打印host基本信息--------------");
        Map<String,String> hostInfo =  dc.getHostInfo();
        System.out.println(hostInfo.get(faird_host_title));
        System.out.println(hostInfo.get(faird_host_port));
        System.out.println(hostInfo.get(faird_host_position));
        System.out.println(hostInfo.get(faird_host_port));
        System.out.println(hostInfo.get(faird_host_name));
        System.out.println(hostInfo.get(faird_host_domain));
        System.out.println(hostInfo.get(faird_tls_enabled));
        System.out.println(hostInfo.get(faird_tls_cert_path));
        System.out.println(hostInfo.get(faird_tls_key_path));

        //获得服务器资源信息
        System.out.println("--------------打印服务器资源信息--------------");
        Map<String,String> serverResourceInfo = dc.getServerResourceInfo();
        System.out.println(serverResourceInfo.get("cpu.cores"));
        System.out.println(serverResourceInfo.get("cpu.usage.percent"));
        System.out.println(serverResourceInfo.get("jvm.memory"));
        System.out.println(serverResourceInfo.get("system.physical.memory"));

        //获得数据帧大小
        System.out.println("--------------打印数据帧大小--------------");
        Long dataFrameSize =  dc.getDataFrameSize("/csv/data_1.csv");
        System.out.println(dataFrameSize);

        //打开非结构化数据的文件列表数据帧
        RemoteDataFrame dfBin = dc.open("/bin");

        //获得数据帧的Document，包含由Provider定义的SchemaURI等信息
        //用户可以控制没有信息时输出的字段
        System.out.println("--------------打印数据帧Document--------------");
        DataFrameDocument dataFrameDocument =  dfBin.getDataFrameDocument();
        String schemaURL = convertToJavaOptional(dataFrameDocument.getSchemaURL()).orElse("schemaURL not found");
        String columnURL = convertToJavaOptional(dataFrameDocument.getColumnURL("file_name")).orElse("columnURL not found");
        String columnAlias = convertToJavaOptional(dataFrameDocument.getColumnAlias("file_name")).orElse("columnAlias not found");
        String columnTitle = convertToJavaOptional(dataFrameDocument.getColumnTitle("file_name")).orElse("columnTitle not found");
        System.out.println(schemaURL);
        System.out.println(columnURL);
        System.out.println(columnAlias);
        System.out.println(columnTitle);
        System.out.println(dfBin.schema());

        //可以对数据帧进行操作 比如foreach 每行数据为一个Row对象，可以通过Tuple风格访问每一列的值
        System.out.println("--------------打印非结构化数据文件列表数据帧--------------");
        dfBin.foreach(row -> {
            //通过Tuple风格访问
            String name = (String) row._1();
            //通过下标访问
            Blob blob = (Blob) row.get(6);
            //除此之外列值支持的类型还包括：Integer, Long, Float, Double, Boolean, byte[]
            //得到blob的InputStream用于读取blob内容
            InputStream inputStream = blob.getInputStream();
            //或者直接将blob写入指定路径和文件名
            String path = Paths.get("src","test","demo","data","output").toString();
            blob.writeToFile(path, name);
            //或者直接获取blob的内容，得到byte数组
            byte[] bytes = blob.content();
            System.out.println(row);
            System.out.println(name);
            System.out.println(blob.size());
            System.out.println(bytes.hashCode());
            return null;
        });



        //还打开Excel文件数据帧
        System.out.println("--------------打印Excel文件数据帧--------------");
        RemoteDataFrameImpl dfExcel = dc.open("/excel/data.xlsx");
        //获取数据
        java.util.List<Row> rows = convertToJavaList(dfExcel.limit(1).collect());
        for(Row row: rows){
            System.out.println(row);
        }

        //还可以打开CSV文件数据帧
        //对数据进行collect操作可以将数据帧的所有行收集到内存中，但是要注意内存溢出的问题
        //limit操作可以限制返回的数据行数，防止内存溢出
        RemoteDataFrame dfCsv = dc.open("/csv/data_1.csv");
        List<Row> rowsCollect = convertToJavaList(dfCsv.limit(1).collect());
        System.out.println("--------------打印结构化数据 /csv/data_1.csv 数据帧--------------");
        for(Row row: rowsCollect){
            System.out.println(row);
        }



        //编写map算子的匿名函数对数据帧进行操作
        //Function1是scala函数类接口，需要实现apply方法并在其中编写函数体
        //SerializableFunction是FairdClient提供的序列化函数接口，继承自Function1
        Function1<Row, Row> mapFunction = new SerializableFunction<Row, Row>() {

            @Override
            public Row apply(Row v1) {
                return Row.apply(convertToScalaSeq(Arrays.asList(v1._1())));
            }

            //两个函数的从右到左组合，一般不用特殊处理
            @Override
            public <A> Function1<A, Row> compose(Function1<A, Row> g) {
                return SerializableFunction.super.compose(g);
            }

            //两个函数的从左到右组合，一般不用特殊处理
            @Override
            public <A> Function1<Row, A> andThen(Function1<Row, A> g) {
                return SerializableFunction.super.andThen(g);
            }
        };
        List<Row> rowsMap = convertToJavaList(dfCsv.limit(3).map(mapFunction).collect());
        System.out.println("--------------打印结构化数据 /csv/data_1.csv 经过map操作后的数据帧--------------");
        for(Row row: rowsMap){
            System.out.println(row);
        }

        //编写filter算子的匿名函数对数据帧进行操作
        Function1<Row, Object> filterFunction = new SerializableFunction<Row, Object>() {

            @Override
            public Object apply(Row row) {
                long id = (long) row._1();
                return id <= 1;
            }

            @Override
            public <A> Function1<A, Object> compose(Function1<A, Row> g) {
                return SerializableFunction.super.compose(g);
            }

            @Override
            public <A> Function1<Row, A> andThen(Function1<Object, A> g) {
                return SerializableFunction.super.andThen(g);
            }
        };
        List<Row> rowsFilter = convertToJavaList(dfCsv.filter(filterFunction).collect());
        System.out.println("--------------打印结构化数据 /csv/data_1.csv 经过filter操作后的数据帧--------------");
        for(Row row: rowsFilter){
            System.out.println(row);
        }


        //自定义算子和DAG执行图对数据帧进行操作
        //构建数据源节点
        DAGNode sourceNodeA = new SourceNode("/csv/data_1.csv");
        //构建自定义算子节点对象
        //自定义一个过滤算子 比如只保留小于等于10的行
        DAGNode udfFilter = new UDFFunction() {
            @Override
            public Iterator<Row> transform(Iterator<Row> iter) {
                return iter.filter(row -> {
                    long value = (long) row._1();
                    return value <= 10;
                });
            }
        };
        //构建节点Map，节点名对应节点对象，可以是数据源节点或者自定义算子节点
        Map<String, DAGNode> javaNodesMap = new HashMap<>();
        javaNodesMap.put("A", sourceNodeA);
        javaNodesMap.put("B", udfFilter);
        //构建边Map，一个节点可以有多个下游节点
        Map<String, List<String>> javaEdgesMap = new HashMap<>();
        javaEdgesMap.put("A", Arrays.asList("B"));
        //通过边和节点Map构建DAG执行图
        TransformerDAG transformerDAG = TransformerDAG.apply(convertToScalaNodesMap(javaNodesMap), convertToScalaEdgesMap(javaEdgesMap));
        //执行DAG图，返回一个数据帧列表
        List<RemoteDataFrame> dfs= convertToJavaList(dc.execute(transformerDAG));
        System.out.println("--------------打印自定义filter算子操作后的数据帧--------------");
        for(RemoteDataFrame df: dfs){
            df.foreach( row ->
            {
                System.out.println(row);
                return null;
            });
        }
    }

    public static <T> java.util.List<T> convertToJavaList(Seq<T> scalaSeq) {
        return JavaConverters.seqAsJavaListConverter(scalaSeq).asJava();
    }

    public static <T> Seq<T> convertToScalaSeq(java.util.List<T> javaList) {
        return JavaConverters.asScalaBufferConverter(javaList).asScala().toSeq();
    }

    public static <T> java.util.Optional<T> convertToJavaOptional(Option<T> scalaOption) {
        return Optional.ofNullable(scalaOption.getOrElse(null));
    }

    public static scala.collection.immutable.Map<String, DAGNode> convertToScalaNodesMap(Map<String, DAGNode> javaMap) {
        return JavaConverters.mapAsScalaMapConverter(javaMap).asScala().toMap(
                scala.Predef.$conforms() // This provides evidence for implicit conversion to scala.Tuple2
        );
    }

    public static scala.collection.immutable.Map<String, Seq<String>> convertToScalaEdgesMap(Map<String, List<String>> javaMap) {
        return JavaConverters.mapAsScalaMapConverter(javaMap).asScala().toMap(scala.Predef.$conforms()).mapValues(
                list -> JavaConverters.asScalaBufferConverter(list).asScala().toSeq()
        );
    }


    public void getExcelDataFrame(){
        FairdClient dc = FairdClient.connect("dacp://10.0.82.71:8232", new UsernamePassword("admin@instdb.cn", "admin001"));

    }

}

