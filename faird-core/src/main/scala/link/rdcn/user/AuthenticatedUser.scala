package link.rdcn.user

import link.rdcn.ConfigLoader
import link.rdcn.util.{CodecUtils, KeyBasedAuthUtils}
import org.json.JSONObject

import java.security.PublicKey

/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/9 17:06
 * @Modified By:
 */
trait AuthenticatedUser

case class KeyAuthenticatedUser(
                                 serverId: String,
                                 nonce: String,
                                 issueTime: Long, //签发时间
                                 validTo: Long, //过期时间
                                 signature: Array[Byte] // UnionServer 私钥签名
                               ) extends AuthenticatedUser {
  def checkPermission(): Boolean = {
    val publicKey: Option[PublicKey] = ConfigLoader.fairdConfig.pubKeyMap.get(serverId)
    if (publicKey.isEmpty) false else {
      if (validTo > issueTime) {
        KeyBasedAuthUtils.verifySignature(publicKey.get, getChallenge(), signature) && System.currentTimeMillis() < validTo
      } else {
        KeyBasedAuthUtils.verifySignature(publicKey.get, getChallenge(), signature)
      }
    }
  }

  private def getChallenge(): Array[Byte] = {
    val jo = new JSONObject().put("serverId", serverId)
      .put("nonce", nonce)
      .put("issueTime", issueTime)
      .put("validTo", validTo)
    CodecUtils.encodeString(jo.toString)
  }
}
