package link.rdcn.struct

import link.rdcn.struct.{Row, StructType}
import link.rdcn.util.{ClosableIterator, DataUtils, JdbcUtils}
import org.json.JSONObject

import java.io.File
import java.nio.file.attribute.BasicFileAttributes
import java.sql.{Connection, DriverManager, ResultSet}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.mutable.ArrayBuffer

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/23 10:14
 * @Modified By:
 */

object DataStreamSourceFactory{

  private val sampleSize = 10
  private val jdbcFetchSize = 500

  def createCsvDataStreamSource(csvFile: File, delimiter: String = ",", header: Boolean = true): DataStreamSource = {
    val fileRowCount = DataUtils.countLinesFast(csvFile)
    val iterLines: ClosableIterator[String] = DataUtils.getFileLines(csvFile)
    val headerArray = new ArrayBuffer[String]()
    if(header) iterLines.next().split(delimiter, -1).map(headerArray.append(_))

    val sampleBuffer = iterLines.take(sampleSize).map(_.split(delimiter, -1)).toArray
    val structType = DataUtils.inferSchema(sampleBuffer, headerArray)

    val sampleRows = sampleBuffer.iterator.map(arr => Row.fromSeq(arr.toSeq))
    val remainingRows = iterLines.map(_.split(delimiter, -1)).map(arr => Row.fromSeq(arr.toSeq))

    val iterRows = (sampleRows ++ remainingRows).map(DataUtils.convertStringRowToTypedRow(_, structType))
    new DataStreamSource {
      override def rowCount: Long = fileRowCount

      override def schema: StructType = structType

      override def iterator: ClosableIterator[Row] = ClosableIterator(iterRows)(iterLines.onClose)
    }
  }


  def createJSONDataStreamSource(jsonFile: File, multiline: Boolean = false): DataStreamSource = {
    val sampleSize = 1
    val fileRowCount = DataUtils.countLinesFast(jsonFile)
    val iterLines: ClosableIterator[String] = DataUtils.getFileLines(jsonFile)
    val sampleBuffer = iterLines.take(sampleSize).toArray
    val headerArray = new ArrayBuffer[String]()
    if (sampleBuffer.nonEmpty) {
      val firstObject = new JSONObject(sampleBuffer.head)
      firstObject.keys().asScala.foreach(key=> headerArray.append(key))
    }
    val sampleObjects = sampleBuffer.map(new JSONObject(_)).iterator.map { jo =>
      // 根据 headerArray 的顺序提取每个 key 对应的值
      headerArray.map(jo.get).map(_.toString).toArray
    }.toArray
    val structType = DataUtils.inferSchema(sampleObjects, headerArray)

    val sampleRows = sampleBuffer.iterator.map(new JSONObject(_)).map { jo =>
      val seq = headerArray.map(jo.get).map(_.toString)
      Row.fromSeq(seq.toSeq)
    }
    val remainingRows = iterLines.map { line =>
      val jo = new JSONObject(line)
      val seq = headerArray.map(jo.get).map(_.toString)
      Row.fromSeq(seq.toSeq)
    }

    val iterRows = (sampleRows ++ remainingRows).map(DataUtils.convertStringRowToTypedRow(_, structType))
    new DataStreamSource {
      override def rowCount: Long = fileRowCount

      override def schema: StructType = structType

      override def iterator: ClosableIterator[Row] = ClosableIterator(iterRows)(iterLines.onClose)
    }
  }

  def createExcelDataStreamSource(excelPath: String): DataStreamSource = {
    val structType = DataUtils.inferExcelSchema(excelPath)
    val iterRows = DataUtils.readExcelRows(excelPath, structType)
    new DataStreamSource {
      override def rowCount: Long = -1

      override def schema: StructType = structType

      override def iterator: ClosableIterator[Row] = ClosableIterator(iterRows.map(Row.fromSeq(_)))()
    }
  }

  def createFileListDataStreamSource(dir: File, recursive: Boolean = false): DataStreamSource = {
    var iterFiles:Iterator[(File, BasicFileAttributes)] = Iterator.empty
    iterFiles = if(recursive) DataUtils.listAllFilesWithAttrs(dir)
    else DataUtils.listFilesWithAttributes(dir).toIterator
    val stream = iterFiles
      // schema [name, byteSize, 文件类型, 创建时间, 最后修改时间, 最后访问时间, file]
      .map{file => (file._1.getName, file._2.size(), DataUtils.getFileTypeByExtension(file._1), file._2.creationTime().toMillis, file._2.lastModifiedTime().toMillis, file._2.lastAccessTime().toMillis,Blob.fromFile(file._1))}
      .map(Row.fromTuple(_))
    new DataStreamSource {
      override def rowCount: Long = -1

      override def schema: StructType = StructType.binaryStructType

      override def iterator: ClosableIterator[Row] = new ClosableIterator(stream, ()=>{},true)
    }
  }


  def createSqlTableDataStreamSource(
                                      jdbcUrl: String,
                                      user: String,
                                      password: String,
                                      tableName: String,
                                      driver: String = "com.mysql.cj.jdbc.Driver"
                                    ): Unit = {
    Class.forName(driver)
    val conn: Connection = DriverManager.getConnection(jdbcUrl, user, password)
    val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    // 可配置 fetchSize 优化大数据查询
    stmt.setFetchSize(jdbcFetchSize)

    val rs = stmt.executeQuery(s"SELECT * FROM $tableName")
    val rsMeta = rs.getMetaData
    val structType = JdbcUtils.inferSchema(rsMeta)
    val iterRows: Iterator[Row] = JdbcUtils.resultSetToIterator(rs, stmt, conn, structType)

    new DataStreamSource {
      override def rowCount: Long = -1 // 行数未知，除非 COUNT 查询

      override def schema: StructType = structType

      override def iterator: ClosableIterator[Row] = ClosableIterator(iterRows)(()=>{
        rs.close()
        stmt.close()
        conn.close()
      })
    }
  }

}