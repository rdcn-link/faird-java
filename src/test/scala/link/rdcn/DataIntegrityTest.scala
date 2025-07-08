package link.rdcn

import link.rdcn.DataFrameOperationTest.isFolderContentsMatch
import link.rdcn.TestBase.{baseDir, binDfInfos, binDir, dc}
import link.rdcn.client.Blob
import link.rdcn.struct.Row
import link.rdcn.util.DataUtils
import link.rdcn.util.SharedValue.getOutputDir
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes

class DataIntegrityTest extends TestBase {
  val outputDir = getOutputDir("test_output\\output").toString

  @ParameterizedTest
  @ValueSource(ints = Array(1))
  def readBinaryTest(num: Int): Unit = {
    val absolutePath = binDir.resolve(s"binary_data_$num.bin")
    val attrs = Files.readAttributes(absolutePath, classOf[BasicFileAttributes])
    val file = absolutePath.toFile
    val expectedRow: Row = {
      val temp = (
        file.getName,                            // name: String
        attrs.size(),                       // size: Long
        DataUtils.getFileTypeByExtension(file),  // 文件类型: String
        attrs.creationTime().toMillis,      // 创建时间: Long (Millis)
        attrs.lastModifiedTime().toMillis,  // 最后修改时间: Long (Millis)
        attrs.lastAccessTime().toMillis,    // 最后访问时间: Long (Millis)
        file)
      Row.fromTuple(temp)
    }
    val expectedName = expectedRow.getAs[String](0).getOrElse(null)
    val expectedSize = expectedRow.getAs[Long](1).getOrElse(null)
    val expectedFileType = expectedRow.getAs[String](2).getOrElse(null)
    val expectedCreatedTime = expectedRow.getAs[Long](3).getOrElse(null)
    val expectedModifiedTime = expectedRow.getAs[Long](4).getOrElse(null)
    val expectedLastAccessTime = expectedRow.getAs[Long](5).getOrElse(null)

    val df = dc.open("/bin")
    df.limit(num).foreach(
      row => {
        println(row)
        val name = row.getAs[String](0).getOrElse(null)
        val size = row.getAs[Long](1).getOrElse(null)
        val fileType = row.getAs[String](2).getOrElse(null)
        val createdTime = row.getAs[Long](3).getOrElse(null)
        val modifiedTime = row.getAs[Long](4).getOrElse(null)
        val lastAccessTime = row.getAs[Long](5).getOrElse(null)
        val blob = row.getAs[Blob](6).getOrElse(null)
        blob.writeToFile(outputDir)
        blob.releaseContentMemory()

        assertEquals(expectedName, name)
        assertEquals(expectedSize, size)
        assertEquals(expectedFileType, fileType)
        assertEquals(expectedCreatedTime, createdTime)
        assertEquals(expectedModifiedTime, modifiedTime)
        assertEquals(expectedLastAccessTime, lastAccessTime)
      }
    )
    assertTrue(isFolderContentsMatch(baseDir.resolve("bin").toString, outputDir), "Binary file mismatch")
  }

}
