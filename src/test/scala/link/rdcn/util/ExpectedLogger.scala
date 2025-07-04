package link.rdcn.util

import link.rdcn.ConfigLoaderTest.getResourcePath
import link.rdcn.util.ExpectedConfigLoader.expectedConfig
import org.apache.logging.log4j.Level

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import java.io.File
import java.nio.file.Files

object ExpectedLogger {
  private val confPath = new File(getResourcePath("faird.conf")).toPath
  private val expectedConfig: Map[String, String] = Files.readAllLines(confPath).asScala
    .filter(_.contains("=")) // 过滤有效行
    .map(_.split("=", 2)) // 按第一个=分割
    .map(arr => arr(0).trim -> arr(1).trim) // 转换为键值对
    .toMap

  def getLevel: Level = Level.toLevel(expectedConfig("logging.level.root"))

  def getFileName: String = expectedConfig("logging.file.name")

  def getConsoleLayout: String = expectedConfig("logging.pattern.console")

  def getFileLayout: String = expectedConfig("logging.pattern.file")

}
