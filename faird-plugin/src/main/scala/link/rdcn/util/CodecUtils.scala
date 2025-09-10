package link.rdcn.util

import link.rdcn.user.{Credentials, UsernamePassword, TokenAuth, SignatureAuth}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
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

  def encodeTicket(typeId: Byte, s: String): Array[Byte] = {
    val b = s.getBytes("UTF-8")
    val buffer = java.nio.ByteBuffer.allocate(1 + 4 + b.length)
    buffer.put(typeId)
    buffer.putInt(b.length)
    buffer.put(b)
    buffer.array()
  }

  def decodeTicket(bytes: Array[Byte]): (Byte, String) = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    val typeId = buffer.get()
    val len = buffer.getInt()
    val b = new Array[Byte](len)
    buffer.get(b)
    (typeId, new String(b, "UTF-8"))
  }

  def encodePairWithTypeId(typeId: Byte, user:String, password: String): Array[Byte] = {
    val userBytes = user.getBytes(StandardCharsets.UTF_8)
    val passwordBytes = password.getBytes(StandardCharsets.UTF_8)

    val buffer: ByteBuffer = ByteBuffer.allocate(1 + 4 + userBytes.length + 4 + passwordBytes.length)
    buffer.put(typeId)
    buffer.putInt(userBytes.length)
    buffer.put(userBytes)
    buffer.putInt(passwordBytes.length)
    buffer.put(passwordBytes)

    buffer.array()
  }

  def decodePairWithTypeId(bytes: Array[Byte]): (Byte, String, String) = {
    val buffer = ByteBuffer.wrap(bytes)
    val typeId = buffer.get()
    val userLen = buffer.getInt()
    val userBytes = new Array[Byte](userLen)
    buffer.get(userBytes)
    val user = new String(userBytes, StandardCharsets.UTF_8)

    val passwordLen = buffer.getInt()
    val passwordBytes = new Array[Byte](passwordLen)
    buffer.get(passwordBytes)
    val password = new String(passwordBytes, StandardCharsets.UTF_8)

    (typeId, user, password)
  }

  val NAME_PASSWORD: Byte = 1
  val TOKEN: Byte = 2
  val ANONYMOUS: Byte = 3
  val SIGNATURE: Byte = 4

  def encodeCredentials(credentials: Credentials): Array[Byte] = {
    credentials match {
      case up: UsernamePassword => encodePairWithTypeId(NAME_PASSWORD, up.username, up.password)
      case token: TokenAuth => encodePairWithTypeId(TOKEN, token.token, "")
      case Credentials.ANONYMOUS => encodePairWithTypeId(ANONYMOUS, "", "")
      case signature: SignatureAuth =>
        val bytes = encodeSignature(signature)
        val buffer: ByteBuffer = ByteBuffer.allocate(1 + bytes.length)
        buffer.put(SIGNATURE)
        buffer.put(bytes)
        buffer.array()
      case _ => throw new IllegalArgumentException(s"$credentials not supported")
    }
  }

  def decodeCredentials(bytes: Array[Byte]): Credentials = {
    lazy val result = decodePairWithTypeId(bytes)
    bytes(0) match {
      case NAME_PASSWORD => UsernamePassword(result._2, result._3)
      case TOKEN => TokenAuth(result._2)
      case ANONYMOUS => Credentials.ANONYMOUS
      case SIGNATURE => decodeSignature(bytes.slice(1, bytes.length))
      case _ => throw new IllegalArgumentException(s"${result._1} not supported")
    }
  }

  /** 将 Signature 编码为字节数组 */
  def encodeSignature(sig: SignatureAuth): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    // 写字符串：先写长度，再写字节
    def writeString(s: String): Unit = {
      val bytes = s.getBytes(StandardCharsets.UTF_8)
      dos.writeInt(bytes.length)
      dos.write(bytes)
    }

    writeString(sig.serverId)
    writeString(sig.nonce)

    dos.writeLong(sig.issueTime)
    dos.writeLong(sig.validTo)

    // 写签名
    dos.writeInt(sig.signature.length)
    dos.write(sig.signature)

    dos.flush()
    bos.toByteArray
  }

  /** 从字节数组解码为 Signature */
  def decodeSignature(bytes: Array[Byte]): SignatureAuth = {
    val dis = new DataInputStream(new ByteArrayInputStream(bytes))

    def readString(): String = {
      val len = dis.readInt()
      val buf = new Array[Byte](len)
      dis.readFully(buf)
      new String(buf, StandardCharsets.UTF_8)
    }

    val serverId = readString()
    val nonce    = readString()
    val issueTime = dis.readLong()
    val validTo = dis.readLong()

    val sigLen = dis.readInt()
    val signature = new Array[Byte](sigLen)
    dis.readFully(signature)

    SignatureAuth(serverId, nonce, issueTime, validTo, signature)
  }

}
