package org.grapheco

import link.rdcn.ProviderTest

import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.io.{BufferedWriter, FileOutputStream, FileWriter}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.util.Using
//import scala.util.Using
import org.apache.commons.io.FileUtils

object TestDataGenerator {
  private val baseDir = ProviderTest.getOutputDir("test_output")
  // 生成的临时目录结构
  private val binDir = ProviderTest.getOutputDir("test_output/bin")
  private val csvDir = ProviderTest.getOutputDir("test_output/csv")
//  private val unstructuredDir = baseDir.resolve("unstructured")

  // 文件数量配置
  private val binFileCount = 3
  private val csvFileCount = 3
//  private val unstructuredFileCount = 3

  // 生成所有测试数据
  def generateTestData(): Unit = {
    println("Starting test data generation...")
    val startTime = System.currentTimeMillis()

    createDirectories()
    generateBinaryFiles()
    generateCsvFiles()
//    generateUnstructuredFiles()

    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Test data generation completed in ${duration}s")
    printDirectoryInfo()
  }

  // 清理所有测试数据
  def cleanupTestData(): Unit = {
    println("Cleaning up test data...")
    val startTime = System.currentTimeMillis()

    if (Files.exists(ProviderTest.getOutputDir("test_output"))) {
      FileUtils.deleteDirectory(baseDir.toFile)
      println(s"Deleted directory: ${baseDir.toAbsolutePath}")
    }

    val duration = (System.currentTimeMillis() - startTime) / 1000.0
    println(s"Cleanup completed in ${duration}s")
  }

  private def createDirectories(): Unit = {
    Files.createDirectories(binDir)
    Files.createDirectories(csvDir)
//    Files.createDirectories(unstructuredDir)
    println(s"Created directory structure at ${baseDir.toAbsolutePath}")
  }

  private def generateBinaryFiles(): Unit = {
    println(s"Generating $binFileCount binary files (~1GB each)...")
    (1 to binFileCount).foreach { i =>
      val fileName = s"binary_data_$i.bin"
      val filePath = binDir.resolve(fileName)
      val startTime = System.currentTimeMillis()
      val size = 1024 * 1024 * 1024 // 1GB
      var fos: FileOutputStream = null
      try {
        fos = new FileOutputStream(filePath.toFile)
        val buffer = new Array[Byte](1024 * 1024) // 1MB buffer
        var bytesWritten = 0L
        while (bytesWritten < size) {
          fos.write(buffer)
          bytesWritten += buffer.length
        }
      } finally {
        if (fos != null) fos.close()
      }
      val duration = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"   Generated ${filePath.getFileName} (${formatSize(size)}) in ${duration}s")
    }
  }


  private def generateCsvFiles(): Unit = {
    println(s"Generating $csvFileCount CSV files with 100 million rows each...")
    (1 to csvFileCount).foreach { i => // 移除 .par，直接按顺序处理
      val fileName = s"data_$i.csv"
      val filePath = csvDir.resolve(fileName)
      val startTime = System.currentTimeMillis()
      val rows = 10000 // 1 亿行
      Using.resource(new BufferedWriter(new FileWriter(filePath.toFile), 1024 * 1024)) { writer =>
        writer.write("id,value\n") // 表头
        for (row <- 1 to rows) {
          writer.append(row.toString).append(',').append(math.random.toString).append('\n')
          if (row % 1000000 == 0) writer.flush() // 每百万行刷一次
        }
      }
      val duration = (System.currentTimeMillis() - startTime) / 1000.0
      println(f"   Generated ${filePath.getFileName} with $rows rows in $duration%.2fs")
    }
  }



  private def formatSize(bytes: Long): String = {
    if (bytes < 1024) s"${bytes}B"
    else if (bytes < 1024 * 1024) s"${bytes / 1024}KB"
    else if (bytes < 1024 * 1024 * 1024) s"${bytes / (1024 * 1024)}MB"
    else s"${bytes / (1024 * 1024 * 1024)}GB"
  }

  private def printDirectoryInfo(): Unit = {
    println("\n Generated Data Summary:")
    printDirectorySize(binDir, "Binary Files")
    printDirectorySize(csvDir, "CSV Files")
//    printDirectorySize(unstructuredDir, "Unstructured Files")
    println("----------------------------------------\n")
  }

  private def printDirectorySize(dir: Path, label: String): Unit = {
    if (Files.exists(dir)) {
      val size = Files.walk(dir)
        .filter(p => Files.isRegularFile(p))
        .mapToLong(p => Files.size(p))
        .sum()
      println(s"   $label: ${formatSize(size)} (${Files.list(dir).count()} files)")
    }
  }
}
