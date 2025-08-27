package link.rdcn.util

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * @Author renhao
 * @Description:
 * @Date 2025/8/25 15:17
 * @Modified By:
 */
object CodecUtils {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def encodePair(user:String, password: String): Array[Byte] = {
    val userBytes = user.getBytes(StandardCharsets.UTF_8)
    val passwordBytes = password.getBytes(StandardCharsets.UTF_8)

    val buffer: ByteBuffer = ByteBuffer.allocate(4 + userBytes.length + 4 + passwordBytes.length)
    buffer.putInt(userBytes.length)
    buffer.put(userBytes)
    buffer.putInt(passwordBytes.length)
    buffer.put(passwordBytes)

    buffer.array()
  }

  def decodePair(bytes: Array[Byte]): (String, String) = {
    val buffer = ByteBuffer.wrap(bytes)

    val userLen = buffer.getInt()
    val userBytes = new Array[Byte](userLen)
    buffer.get(userBytes)
    val user = new String(userBytes, StandardCharsets.UTF_8)

    val passwordLen = buffer.getInt()
    val passwordBytes = new Array[Byte](passwordLen)
    buffer.get(passwordBytes)
    val password = new String(passwordBytes, StandardCharsets.UTF_8)

    (user, password)
  }

  def encodeWithMap(data: Array[Byte], params: Map[String, Any]): Array[Byte] = {
    val mapBytes = mapper.writeValueAsBytes(params) // Map -> JSON Bytes
    val buffer = ByteBuffer.allocate(4 + data.length + 4 + mapBytes.length)

    buffer.putInt(data.length)
    buffer.put(data)
    buffer.putInt(mapBytes.length)
    buffer.put(mapBytes)

    buffer.array()
  }


  def decodeWithMap(bytes: Array[Byte]): (Array[Byte], Map[String, Any]) = {
    val buffer = ByteBuffer.wrap(bytes)

    val dataLen = buffer.getInt()
    val dataBytes = new Array[Byte](dataLen)
    buffer.get(dataBytes)

    val mapLen = buffer.getInt()
    val mapBytes = new Array[Byte](mapLen)
    buffer.get(mapBytes)

    val params = mapper.readValue(mapBytes, classOf[Map[String, Any]])
    (dataBytes, params)
  }

  def encodeMap(data: Map[String, Any]): Array[Byte] = mapper.writeValueAsBytes(data)

  def decodeMap(mapBytes: Array[Byte]): Map[String, Any] = mapper.readValue(mapBytes, classOf[Map[String, Any]])


  /** 把字符串编码成字节数组 */
  def encodeString(str: String): Array[Byte] = {
    if (str == null) Array.emptyByteArray
    else str.getBytes(StandardCharsets.UTF_8)
  }

  /** 把字节数组解码成字符串 */
  def decodeString(bytes: Array[Byte]): String = {
    if (bytes == null || bytes.isEmpty) ""
    else new String(bytes, StandardCharsets.UTF_8)
  }

  val BLOB_STREAM: Byte = 1
  val URL_STREAM: Byte = 2

  def encodeTicket(typeId: Byte, s1: String, s2: String): Array[Byte] = {
    val b1 = s1.getBytes("UTF-8")
    val b2 = s2.getBytes("UTF-8")
    val buffer = java.nio.ByteBuffer.allocate(1 + 4 + b1.length + 4 + b2.length)
    buffer.put(typeId)
    buffer.putInt(b1.length)
    buffer.put(b1)
    buffer.putInt(b2.length)
    buffer.put(b2)
    buffer.array()
  }

  def decodeTicket(bytes: Array[Byte]): (Byte, String, String) = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val typeId = buffer.get()
    val len1 = buffer.getInt()
    val b1 = new Array[Byte](len1)
    buffer.get(b1)
    val len2 = buffer.getInt()
    val b2 = new Array[Byte](len2)
    buffer.get(b2)
    (typeId, new String(b1, "UTF-8"), new String(b2, "UTF-8"))
  }

}
