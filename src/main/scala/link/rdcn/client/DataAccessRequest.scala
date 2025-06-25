package link.rdcn.client

import link.rdcn.struct.StructType
import link.rdcn.user.Credentials

import java.io.Serializable

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/19 10:20
 * @Modified By:
 */
case class DataAccessRequest(
                              dataFrame: String,
                              userCredential: Credentials,                      // 用户凭证
                            ) extends Serializable

