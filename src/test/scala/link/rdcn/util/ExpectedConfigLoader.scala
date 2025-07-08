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

  def getHostName: String = expectedConfig("faird.hostName")

  def getHostTitle: String = expectedConfig("faird.hostTitle")

  def getHostPosition: String = expectedConfig("faird.hostPosition")

  def getHostDomain: String = expectedConfig("faird.hostDomain")

  def getHostPort: Int = expectedConfig("faird.hostPort").toInt

  def getCatdbPort: Int = expectedConfig("faird.catdbPort").toInt

}

