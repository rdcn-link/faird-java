package link.rdcn;

import link.rdcn.provider.DataProvider;
import link.rdcn.provider.DataStreamSource;
import link.rdcn.provider.DataStreamSourceFactory;
import link.rdcn.server.FairdServer;
import link.rdcn.struct.*;
import link.rdcn.user.AuthProvider;
import link.rdcn.user.AuthenticatedUser;
import link.rdcn.user.Credentials;
import link.rdcn.server.exception.AuthException;
import link.rdcn.util.DataUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.*;

import java.io.IOException;
import java.util.*;

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

    public static void main(String[] args) throws IOException, InterruptedException {

        InputSource inputSource = new CSVSource(",", false);
        DataFrameInfo dfInfo = new DataFrameInfo("/home/dd/file_1.csv", inputSource, StructType.empty().add("id", ValueTypeHelper.getIntType(), true)
                .add("name", ValueTypeHelper.getStringType(), true));
        List<DataFrameInfo> listD = new ArrayList<DataFrameInfo>();
        listD.add(dfInfo);
        DataSet ds1 = new DataSet("dd", "1", JavaConverters.asScalaBufferConverter(listD).asScala().toList());

        ConfigLoader.init(JProviderTest.class.getClassLoader().getResource("faird.conf").getPath());

        Location location = Location.forGrpcInsecure(ConfigBridge.getConfig().getHostPosition(), ConfigBridge.getConfig().getHostPort());
        BufferAllocator allocator = new RootAllocator();


        // 创建AuthenticatedUser的实现类，用于存储用户登陆凭证，这里通过记录token作为示例
        class TestAuthenticatedUser implements AuthenticatedUser {
            private final String token;

            /**
             * 构造方法，用于初始化成员变量token
             * @param token
             */
            TestAuthenticatedUser(String token) {
                this.token = token;
            }
        }

        // 创建AuthProvider的实现类，用于处理用户认证和授权逻辑
        AuthProvider authProvider = new AuthProvider() {
            /**
             * 使用提供的凭证进行用户身份验证
             *
             * @param credentials 用户凭证对象，包含用户的登录信息。
             * @return 一个经过验证的用户对象。
             * @throws AuthException 如果身份验证失败，则抛出此类异常，需要Provider按需求实现。
             */
            @Override
            public AuthenticatedUser authenticate(Credentials credentials) throws AuthException {
                return new TestAuthenticatedUser(UUID.randomUUID().toString());
            }

            /**
             * 授权方法，用于判断用户是否有权限访问指定的DataFrame。
             *
             * @param user          已认证的用户对象
             * @param dataFrameName 需要访问的DataFrame名称
             * @return 如果用户有权限访问指定的DataFrame，则返回true；否则返回false，需要Provider按需求实现。
             */
            @Override
            public boolean authorize(AuthenticatedUser user, String dataFrameName) {
                return true;
            }
        };


        // 创建DataProvider的实例，用于处理DataSet和DataFrame的获取逻辑
        DataProvider dataProvider = new DataProvider() {


            /**
             * 获取数据集名称列表
             * Provider实现该方法以提供DataSet名称列表
             *
             * @return 返回数据集名称的列表
             */
            @Override
            public List<String> listDataSetNames() {


                return Collections.emptyList();

            }

            /**
             * 获取DataSet元数据
             * Provider通过配置文件和DataSetName获得URI和DataSetID
             * Provider提供DataSet和空RDF模型对象，并需要实现对RDF信息的注入
             * 元数据将从rdfModel获取
             *
             * @param dataSetName DataSet名称
             * @param rdfModel    空RDF模型对象
             */
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
                List<String> dataFrameNames = new ArrayList<>();
                dataFrameNames.add("123123");
                dataFrameNames.add("dsfsdf");
                for (String dataFrameName : dataFrameNames) {
                    datasetRes.addProperty(hasFile, dataFrameName);
                }
            }

            /**
             * 列出指定DataSet的DataFrame名称列表
             *
             * @param dataSetName 数据集名称
             * @return 数据帧名称列表，如果数据集不存在或数据帧列表为空，则返回空列表
             */
            @Override
            public List<String> listDataFrameNames(String dataSetName) {
                return Collections.emptyList();
            }

            /**
             * 通过DataFrame的名称，根据DataStreamSourceFactory获取DataStreamSource
             * Provider应当实现DataStreamSourceFactory静态类，根据不同的文件类型获得DataStreamSource
             * 注意：Factory应该设计针对读取一个文件夹下二进制文件的情况，此时DataFrame名称应为文件夹名称而非文件名
             * 并且Schema应该定义为：[name, size, 文件类型, 创建时间, 最后修改时间, 最后访问时间, file]
             * 对于其他文件类型，Row的每列应与Schema一一对应
             * DataStreamSource类提供将数据处理为Iterator[Row]的方式，Provider也需要实现此类
             * 此处示例为通过Name、Schema、文件类型（csv，json，bin...）获得DataStreamSource
             *
             * DataStreamSource的实现类，用于流式提供DataFrame
             * process方法产生用于传输DataFrame的迭代器（此处使用Flight协议作为示例）
             * createDataFrame方法用于组装DataFrame对象，实例中包含schema和迭代器作为示例
             * DataStreamSource dataStreamSource = new DataStreamSource() {
             *             @Override
             *             public Iterator<ArrowRecordBatch> process(Iterator<Row> stream, VectorSchemaRoot root, int batchSize) {
             *                 return null;
             *             }
             *
             *             @Override
             *             public DataFrame createDataFrame() {
             *                 return null;
             *             }
             *         };
             *
             * @param dataFrameName DataFrame的名称
             * @return DataStreamSource
             */
            @Override
            public DataStreamSource getDataFrameSource(String dataFrameName) {
                DataUtils.inferExcelSchema("");
                DataUtils.readExcelRows("", DataUtils.inferExcelSchema(""));
                DirectorySource directorySource = new DirectorySource(false);
                return DataStreamSourceFactory.getDataFrameSourceFromInputSource(dataFrameName, getDataFrameSchema(dataFrameName), directorySource);
            }



            /**
             * 获取指定名称的DataFrame的Schema。
             * 此处展示了生成Schema的方法，实际需要Provider提供DataFrame名称到对应Schema的映射
             *
             * @param dataFrameName DataFrame的名称
             * @return DataFrame的Schema，类型为 {@link StructType}
             */
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

            /**
             * 根据DataFrame名称获取DataFrameSchema的URL
             * 该URL唯一确定数据帧Schema，由Provider定义对应规则
             *
             * @param dataFrameName DataFrame的名称
             * @return DataFrameSchema的URL
             */
            @Override
            public String getDataFrameSchemaURL(String dataFrameName) {
                String hostname = ConfigBridge.getConfig().getHostName(); //配置文件中 faird.hostName=cerndc
                int port = ConfigBridge.getConfig().getHostPort(); //faird.hostPort=3101
                String dataFrameSchemaURL = "dacp://" + hostname + ":" + port + "/" + dataFrameName;
                return dataFrameSchemaURL;
            }
        };

        //设置一个路径存放faird相关外部文件，其中faird.conf 存放到 $fairdHome/conf 路径下
        String fairdHome = "./";
        FairdServer fairdServer = new FairdServer(dataProvider, authProvider, fairdHome);

        //启动faird服务
        fairdServer.start();
        //关闭faird服务
        fairdServer.close();
    }

}
