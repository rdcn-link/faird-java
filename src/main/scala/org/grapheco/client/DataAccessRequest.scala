package org.grapheco.client

import org.apache.spark.sql.types.StructType
import org.grapheco.Credentials

import java.io.Serializable

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/19 10:20
 * @Modified By:
 */
case class DataAccessRequest(
                              datasetId: String,                                // 数据集唯一标识
                              dataFrames: String,                               // DataFrame名称 -> 来源
                              userCredential: Credentials,                      // 用户凭证
                              expectedSchema: StructType,                      // DataFrame名称 -> 期望的schema
                              inputSource: InputSource = StructuredSource() //默认读取结构化数据，DataFrame只有一列，内容为一行
                            ) extends Serializable

sealed trait InputSource extends Serializable
case class CSVSource(
                      delimiter: String = ","
                    ) extends InputSource

case class JSONSource(
                       multiline: Boolean = false,
                       schema: Option[StructType] = None
                     ) extends InputSource

case class DirectorySource(
                            recursive: Boolean = true
                          ) extends InputSource

case class StructuredSource() extends InputSource

