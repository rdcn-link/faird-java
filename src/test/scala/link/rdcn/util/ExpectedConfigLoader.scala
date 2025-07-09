package link.rdcn.util

import link.rdcn.ConfigLoaderTest.getResourcePath

import java.io.File
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import java.nio.file.{Files, Paths}

object ExpectedConfigLoader {

  private val confPath = new File(getResourcePath("faird.conf")).toPath
  private val expectedConfig: Map[String, String] = Files.readAllLines(confPath).asScala
    .filter(_.contains("=")) // 过滤有效行
    .map(_.split("=", 2)) // 按第一个=分割
    .map(arr => arr(0).trim -> arr(1).trim) // 转换为键值对
    .toMap

  def getHostName: String = expectedConfig("faird.host.name")

  def getHostTitle: String = expectedConfig("faird.host.title")

  def getHostPosition: String = expectedConfig("faird.host.position")

  def getHostDomain: String = expectedConfig("faird.host.domain")

  def getHostPort: Int = expectedConfig("faird.host.port").toInt

  def getCatdbPort: Int = expectedConfig("faird.catdb.port").toInt

}

