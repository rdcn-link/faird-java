package link.rdcn.dftree

import link.rdcn.SimpleSerializer
import link.rdcn.client.DFOperation
import org.json.JSONObject


/**
 * @Author renhao
 * @Description:
 * @Data 2025/7/2 15:40
 * @Modified By:
 */

sealed trait FunctionWrapper {
  def toJson: JSONObject
}

object FunctionWrapper {

  case class JsonCode(code: String) extends FunctionWrapper {
    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", "JsonCode")
      jo.put("code", code)
    }
    override def toString(): String = "JsonCode Function"
  }

  case class JavaSerialized(serializedBase64: String) extends FunctionWrapper {
    override def toJson: JSONObject = {
      val jo = new JSONObject()
      jo.put("type", "JavaSerialized")
      jo.put("serializedBase64", serializedBase64)
    }
    override def toString(): String = "JavaSerialized Function"
  }

  def apply(jsonObj: JSONObject): FunctionWrapper = {
    val funcType = jsonObj.getString("type")
    funcType match {
      case "JsonCode" => JsonCode(jsonObj.getString("code"))
      case "JavaSerialized" => JavaSerialized(jsonObj.getString("serializedBase64"))
    }
  }

  def getJavaSerialized(dfOperation: DFOperation): JavaSerialized = {
    val objectBytes = SimpleSerializer.serialize(dfOperation)
    val base64Str: String = java.util.Base64.getEncoder.encodeToString(objectBytes)
    JavaSerialized(base64Str)
  }
}

