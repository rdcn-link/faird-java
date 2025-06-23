package link.rdcn.client

import link.rdcn.Logging
import org.apache.hadoop.shaded.com.sun.jersey.core.header.MatchingEntityTag
import org.apache.jena.rdf.model.{Literal, Model, Resource}
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructType}

import scala.collection.mutable

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/10 17:24
 * @Modified By:
 */
trait SerializableFunction[-T, +R] extends (T => R) with Serializable

trait RemoteDataFrame extends Serializable {
  val source: DataAccessRequest
  val ops: List[DFOperation]

  def setSchema(schema: String): Unit

  def setSchemaURI(schemaURI: String): Unit

  def setMetaData(metaData: String): Unit

  def setRDFModel(rdfModel: Model): Unit

  def setPropertiesMap: Unit

  def getSchema: String

  def getSchemaURI: String

  def getMetaData: String

  def getRDFModel: Model

  def getPropertiesMap: Map[String ,String]

  def map(f: Row => Row): RemoteDataFrame

  def filter(f: Row => Boolean): RemoteDataFrame

  def select(columns: String*): RemoteDataFrame

  def limit(n: Int): RemoteDataFrame

  def reduce(f: ((Row, Row)) => Row): RemoteDataFrame

  def foreach(f: Row => Unit): Unit // 远程调用 + 拉取结果

  def collect(): List[Row]
}

case class GroupedDataFrame(remoteDataFrameImpl: RemoteDataFrameImpl) {
  def max(column: String): RemoteDataFrameImpl = {
    RemoteDataFrameImpl(remoteDataFrameImpl.source, remoteDataFrameImpl.ops :+ MaxOp(column), remoteDataFrameImpl.client)
  }
  //可自定义聚合函数
}

case class RemoteDataFrameImpl(source: DataAccessRequest, ops: List[DFOperation], client: ArrowFlightClient = null) extends RemoteDataFrame with Logging {
  private var _schema: String = _
  private var _format: String = _
  private var _metaData: String = _
  private var _schemaURI: String = _
  private var _rdfModel: Model = _
  private var _propertiesMap: Map[String, String] = Map.empty[String, String]

  override def filter(f: Row => Boolean): RemoteDataFrame = {
    copy(ops = ops :+ FilterOp(new SerializableFunction[Row, Boolean] {
      override def apply(v1: Row): Boolean = f(v1)
    }))
  }

  override def select(columns: String*): RemoteDataFrame = {
    copy(ops = ops :+ SelectOp(columns))
  }

  override def limit(n: Int): RemoteDataFrame = {
    copy(ops = ops :+ LimitOp(n))
  }

  override def foreach(f: Row => Unit): Unit = records.foreach(f)

  override def collect(): List[Row] = records.toList

  override def map(f: Row => Row): RemoteDataFrame = {
    copy(ops = ops :+ MapOp(new SerializableFunction[Row, Row] {
      override def apply(v1: Row): Row = f(v1)
    }))
  }

  override def reduce(f: ((Row, Row)) => Row): RemoteDataFrame = {
    copy(ops = ops :+ ReduceOp(new SerializableFunction[(Row, Row), Row] {
      override def apply(v1: (Row, Row)): Row = f(v1)
    }))
  }

  def groupBy(column: String): GroupedDataFrame = {
    copy(ops = ops :+ GroupByOp(column))
    GroupedDataFrame(this)
  }

  private def records(): Iterator[Row] = client.getRows(source, ops)

  override def getSchema: String = {
    if(_schema == null)
      _schema=client.getSchema(source.datasetId)
    _schema
  }

  override def getMetaData: String = {
    if(_metaData == null)
      _metaData=client.getMetaData(source.datasetId)
    _metaData
  }

  override def getSchemaURI: String = {
    if(_schemaURI == null)
      _schemaURI=client.getSchemaURI(source.datasetId)
    _schemaURI
  }

  override def setSchema(schema: String): Unit = {
    _schema = schema
//    _format = _propertiesMap("http://example.org/dataset/"+"format").toString
//    _schema= _format match {
//      case "csv" =>{
//        new StructType()
//          .add("col1", StringType)
//          .add("col2", StringType)
//      }
//      case "structrued" => {
//        new StructType()
//          .add("name", StringType)
//      }
//      case _ => {
//        new StructType()
//          .add("name", StringType)
//          .add("path", StringType)
//          .add("ext", StringType)
//          .add("type", StringType)
//          .add("size", StringType)
//          .add("lastModiefied", StringType)
//          .add("bin", BinaryType)
//      }
//    }
  }

  override def setSchemaURI(schemaURI: String): Unit = _schemaURI=schemaURI

  override def setMetaData(metaData: String): Unit = _metaData = metaData

  override def setRDFModel(rdfModel: Model): Unit = {
    _rdfModel = rdfModel
  }

  override def getRDFModel: Model = _rdfModel

  override def setPropertiesMap: Unit = {
    val resourceUri = "http://example.org/dataset/"  + source.datasetId
    val subject: Resource = _rdfModel.getResource(resourceUri)
    val properties = mutable.LinkedHashMap[String, String]()  // 保持属性顺序（可选）
    // 遍历该资源的所有 Statement（三元组）
    val statements = _rdfModel.listStatements(subject, null, null)
    while (statements.hasNext) {
      val stmt = statements.nextStatement()
      val propName = stmt.getPredicate.getLocalName // 获取属性名（无URI前缀）
      val obj = stmt.getObject
      // 转换属性值为合适的类型
      val propValue = obj match {
        case lit: Literal => lit.getString  // 字符串值
        case res: Resource if res.isURIResource => res.getURI  // 其他资源（URI）
        case _ => obj.toString  // 回退方案
      }
//      log.info(s"$propName->$propValue")
      properties += (propName -> propValue)
    }
//    log.info(s"${properties("dataFormat").asInstanceOf[String]}")
    _propertiesMap = properties.toMap
  }

  override def getPropertiesMap: Map[String, String] = _propertiesMap
}



