package link.rdcn

import link.rdcn.client.dag.{DAGNode, SourceNode, TransformerDAG, UDFFunction}
import link.rdcn.client.{Blob, DataFrame, FairdClient, RemoteDataFrame}
import link.rdcn.provider.DataFrameDocument
import link.rdcn.struct.Row
import link.rdcn.user.UsernamePassword
import org.apache.commons.io.IOUtils
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileOutputStream
import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._

/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/22 10:49
 * @Modified By:
 */
object ClientDemo {
  private val log: Logger = LoggerFactory.getLogger(classOf[JClientDemo])

  def main(args: Array[String]): Unit = {
    // 通过用户名密码非加密连接FairdClient
    // FairdClient dc = FairdClient.connect("dacp://localhost:3101", new UsernamePassword("admin@instdb.cn", "admin001"));
    // 通过用户名密码tls加密连接FairdClient 需要用户端进行相关配置
    val dc: FairdClient = FairdClient.connectTLS("dacp://localhost:3101", UsernamePassword("admin@instdb.cn", "admin001"))
    // 匿名连接FairdClient
    // FairdClient dcAnonymous = FairdClient.connect("dacp://localhost:3101", Credentials.ANONYMOUS());

    //获得所有的数据集名称
    println("--------------打印数据集列表--------------")
    val dataSetNames: Seq[String] = dc.listDataSetNames()
    dataSetNames.foreach(println)

    //获得指定数据集的所有的数据帧名称
    println("--------------打印数据集 csv 所有数据帧名称--------------")
    val frameNames: Seq[String] = dc.listDataFrameNames("csv")
    frameNames.foreach(println)

    //获得指定数据集的元数据信息
    println("--------------打印数据集 csv 的元数据信息--------------")
    val metaData: String = dc.getDataSetMetaData("csv")
    println(metaData)
    //返回Model

    //获得host基本信息
    println("--------------打印host基本信息--------------")
    val hostInfo: Map[String, String] = dc.getHostInfo
    println(hostInfo(ConfigKeys.FairdHostName))
    println(hostInfo(ConfigKeys.FairdHostTitle))
    println(hostInfo(ConfigKeys.FairdHostPort))
    println(hostInfo(ConfigKeys.FairdHostPosition))
    println(hostInfo(ConfigKeys.FairdHostDomain))
    println(hostInfo(ConfigKeys.FairdTlsEnabled))
    println(hostInfo(ConfigKeys.FairdTlsCertPath))
    println(hostInfo(ConfigKeys.FairdTlsKeyPath))
    println(hostInfo(ConfigKeys.LoggingFileName))
    println(hostInfo(ConfigKeys.LoggingLevelRoot))
    println(hostInfo(ConfigKeys.LoggingPatternConsole))
    println(hostInfo(ConfigKeys.LoggingPatternFile))

    //获得服务器资源信息
    println("--------------打印服务器资源信息--------------")
    val serverResourceInfo: Map[String, String] = dc.getServerResourceInfo
    println(serverResourceInfo(ResourceKeys.CpuCores))
    println(serverResourceInfo(ResourceKeys.CpuUsagePercent))
    println(serverResourceInfo(ResourceKeys.JvmMaxMemory))
    println(serverResourceInfo(ResourceKeys.JvmTotalMemory))
    println(serverResourceInfo(ResourceKeys.JvmUsedMemory))
    println(serverResourceInfo(ResourceKeys.JvmFreeMemory))
    println(serverResourceInfo(ResourceKeys.SystemMemoryTotal))
    println(serverResourceInfo(ResourceKeys.SystemMemoryUsed))
    println(serverResourceInfo(ResourceKeys.SystemMemoryFree))



    //打开非结构化数据的文件列表数据帧
    val dfBin: RemoteDataFrame = dc.open("/bin")

    //获得数据帧的Document，包含由Provider定义的SchemaURI等信息
    //用户可以控制没有信息时输出的字段
    println("--------------打印数据帧Document--------------")
    val dataFrameDocument: DataFrameDocument = dfBin.getDocument
    val schemaURL: String = dataFrameDocument.getSchemaURL().getOrElse("schemaURL not found")
    val columnURL: String = dataFrameDocument.getColumnURL("file_name").getOrElse("columnURL not found")
    val columnAlias: String = dataFrameDocument.getColumnAlias("file_name").getOrElse("columnAlias not found")
    val columnTitle: String = dataFrameDocument.getColumnTitle("file_name").getOrElse("columnTitle not found")
    println(schemaURL)
    println(columnURL)
    println(columnAlias)
    println(columnTitle)
    println(dfBin.schema)

    //获得数据帧大小
    println("--------------打印数据帧大小--------------")
    val dataFrameRowCount: Long = dfBin.getStatistics.rowCount
    val dataFrameSize: Long = dfBin.getStatistics.size
    println(dataFrameRowCount)
    println(dataFrameSize)


    //可以对数据帧进行操作 比如foreach 每行数据为一个Row对象，可以通过Tuple风格访问每一列的值
    println("--------------打印非结构化数据文件列表数据帧--------------")
    dfBin.foreach((row: Row) => {
      //通过Tuple风格访问
      val name: String = row._1.asInstanceOf[String]
      //通过下标访问
      val blob: Blob = row.get(6).asInstanceOf[Blob]
      //除此之外列值支持的类型还包括：Integer, Long, Float, Double, Boolean, byte[]
      //offer用于接受一个用户编写的处理blob InputStream的函数并确保其关闭
      val path: Path = Paths.get("src", "test", "demo", "data", "output", name)
      blob.offer(inputStream => {
        val outputStream = new FileOutputStream(path.toFile)
        IOUtils.copy(inputStream, outputStream)
        outputStream.close()
      })
      //或者直接获取blob的内容，得到byte数组
      //由于offer后blob被消费，此时调用会抛出异常
      //val bytes: Array[Byte] = blob.toBytes
      println(row)
      println(name)
      println(blob.size)
    })

    //获取数据
    //对数据进行collect操作可以将数据帧的所有行收集到内存中，但是要注意内存溢出的问题
    //limit操作可以限制返回的数据行数，防止内存溢出
    //还可以打开CSV文件数据帧
    val dfCsv: DataFrame = dc.open("/csv/data_1.csv")
    val csvRows: Seq[Row] = dfCsv.limit(1).collect()
    println("--------------打印结构化数据 /csv/data_1.csv 数据帧--------------")
    csvRows.foreach(println)

    //编写map算子的匿名函数对数据帧进行操作
    val rowsMap: Seq[Row] = dfCsv.limit(3).map(x=>Row(x._1)).collect()
    println("--------------打印结构化数据 /csv/data_1.csv 经过map操作后的数据帧--------------")
    rowsMap.foreach(println)

    //编写filter算子的匿名函数对数据帧进行操作
    val rowsFilter: Seq[Row] = dfCsv.filter({ row =>
      val id: Long = row._1.asInstanceOf[Long]
      id <= 1L
    }).collect()
    println("--------------打印结构化数据 /csv/data_1.csv 经过filter操作后的数据帧--------------")
    rowsFilter.foreach(println)
    //自定义算子和DAG执行图对数据帧进行操作
    //构建数据源节点
    val sourceNodeA: DAGNode = new SourceNode("/csv/data_1.csv")
    val sourceNodeB: DAGNode = new SourceNode("/csv/data_2.csv")

    //也可以构建自定义算子节点对象
    //自定义一个map算子 比如对第一列加1
    val udfMap: DAGNode = new UDFFunction {
      override def transform(iter: Iterator[Row]): Iterator[Row] = {
        iter.map(row => Row.fromTuple(row.getAs[Long](0).get + 1, row.get(1)))
      }
    }

    //自定义一个filter算子 比如只保留小于等于3的行
    val udfFilter: DAGNode = new UDFFunction() {
      //DataFrame
      override def transform(iter: Iterator[Row]): Iterator[Row] = iter.filter((row: Row) => {
        val value: Long = row._1.asInstanceOf[Long]
        value <= 3L
      })
    }


    //构建节点Map，节点名对应节点对象，可以是数据源节点或者自定义算子节点
    //至少一个节点
    val nodesMapSingle: Map[String, DAGNode] = Map(
      "A" -> sourceNodeA
    )
    //构建边Map，可以没有边，对应不进行操作
    val edgesMapMin: Map[String, Seq[String]] = Map.empty
    //通过边和节点Map构建DAG执行图
    val transformerDAGMin: TransformerDAG = TransformerDAG(nodesMapSingle, edgesMapMin)
    //执行DAG图，返回一个数据帧列表
    val minDfs: Seq[DataFrame] = dc.execute(transformerDAGMin)
    println("--------------打印最小DAG直接获取的数据帧--------------")
    minDfs.foreach(df => df.foreach(row => println(row))
    )


    //构建DAG执行图A -> B ，A是数据源节点B是自定义filter算子
    val nodesMap: Map[String, DAGNode] = Map(
      "A" -> sourceNodeA,
      "B" -> udfFilter
    )
    //构建边Map，一个节点可以有多个下游节点
    val edgesMap: Map[String, Seq[String]] = Map(
      "A" -> Seq("B")
    )
    //通过边和节点Map构建DAG执行图
    val transformerDAG: TransformerDAG = TransformerDAG(nodesMap, edgesMap)
    //执行DAG图，返回一个数据帧列表
    val simpleDfs: Seq[DataFrame] = dc.execute(transformerDAG)
    println("--------------打印自定义filter算子操作后的数据帧--------------")
    simpleDfs.foreach(df => df.foreach(row => println(row))
    )

    //对于线性依赖也可以通过fromSeq构造DAG
    val transformerDAGSeq: TransformerDAG = TransformerDAG.fromSeq(nodesMap, Seq("A", "B"))
    val seqDAGDfs: Seq[DataFrame] = dc.execute(transformerDAG)
    println("--------------打印执行直接构造线性依赖DAG后的数据帧--------------")
    seqDAGDfs.foreach(df => df.foreach(row => println(row))
    )

    //可以构建更复杂的多数据源节点和操作的DAG
    val transformerComplexDAG: TransformerDAG = TransformerDAG(
      Map("A" -> sourceNodeA,
        "B" -> sourceNodeB,
        "C" -> udfMap,
        "D" -> udfFilter),
      Map("A" -> Seq("C"),
        "B" -> Seq("C"),
        "C" -> Seq("D"))
    )
    val complexDfs: Seq[DataFrame] = dc.execute(transformerComplexDAG)
    println("--------------打印执行自定义复杂DAG后的数据帧--------------")
    complexDfs.foreach(df => df.foreach(row => println(row)))
  }

}
