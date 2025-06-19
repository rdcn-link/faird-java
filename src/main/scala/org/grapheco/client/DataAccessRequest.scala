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
                              expectedSchema: StructType                        // DataFrame名称 -> 期望的schema
                            ) extends Serializable

