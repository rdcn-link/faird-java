/**
 * @Author Yomi
 * @Description:
 * @Data 2025/7/30 17:03
 * @Modified By:
 */
package link.rdcn.dftree

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{FileIO, Source}
import akka.util.ByteString
import org.json.JSONObject

import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

class OperatorClient(host: String = "localhost", port: Int = 8088, implicit val system: ActorSystem = ActorSystem("HttpClient")) {

  def getOperatorInfo(functionId: String): Future[JSONObject] = {
    val downloadUrl = s"http://10.0.89.38:8088/fileInfo?id=$functionId"
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = downloadUrl
    )

    Http().singleRequest(request).flatMap { response =>
      if (response.status.isSuccess()) {

        response.entity.toStrict(5.seconds).map(_.data.utf8String).flatMap { jsonString =>
          try {
            val jsonObject = new JSONObject(jsonString)
            Future.successful(jsonObject)
          } catch {
            case ex: Exception =>
              Future.failed(new RuntimeException(s"JSON 解析失败: ${ex.getMessage}. 原始响应体: $jsonString", ex))
          }
        }
      } else {
        response.entity.toStrict(5.seconds).map(_.data.utf8String).flatMap { body =>
          Future.failed(new RuntimeException(s"请求失败，状态码: ${response.status}, 响应体: $body"))
        }.recoverWith {
          case ex: Exception => Future.failed(new RuntimeException(s"请求失败，状态码: ${response.status}, 无法获取响应体: ${ex.getMessage}", ex))
        }
      }
    }
  }


  def uploadPackage(filePath: String, functionId: String, fileType: String, desc: String, functionName: String): Future[String] = {
    val file = new File(filePath)

    if (!file.exists()) {
      println(s"错误: 文件不存在于路径: $filePath")
      Future.failed(new IllegalArgumentException(s"错误: 文件不存在于路径: $filePath"))
    }

    // 创建文件上传的 ByteString 源
    val fileSource = FileIO.fromPath(file.toPath)

    // 构建 multipart/form-data
    val formData = Multipart.FormData(
      Source(
        List(
          // 'id' 字段
          Multipart.FormData.BodyPart.Strict(
            "id",
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(functionId))
          ),
          // 'file' 字段，包含文件内容和文件名
          Multipart.FormData.BodyPart(
            "file",
            HttpEntity(ContentTypes.`application/octet-stream`, file.length(), fileSource),
            Map("filename" -> file.getName) // 设置文件名
          ),
          // 'type' 字段
          Multipart.FormData.BodyPart.Strict(
            "type",
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(fileType))
          ),
          // 'desc' 字段
          Multipart.FormData.BodyPart.Strict(
            "desc",
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(desc))
          ),
          // 'functionName' 字段
          Multipart.FormData.BodyPart.Strict(
            "functionName",
            HttpEntity(ContentTypes.`text/plain(UTF-8)`, ByteString(functionName))
          )
        )
      )
    )

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = "http://10.0.89.38:8088/uploadPackage",
      entity = formData.toEntity()
    )
    Http().singleRequest(request).flatMap { response =>
      if (response.status.isSuccess()) {
        response.entity.toStrict(5.seconds).map(_.data.utf8String)
        Future.successful("success")
      } else {
        response.entity.toStrict(5.seconds).map(_.data.utf8String).flatMap { body =>
          Future.failed(new RuntimeException(s"上传失败，状态码: ${response.status}, 响应体: $body"))
        }.recoverWith {
          case ex: Exception => Future.failed(new RuntimeException(s"上传失败，状态码: ${response.status}, 无法获取响应体: ${ex.getMessage}", ex))
        }
      }
    }
  }


  def downloadPackage(functionId: String, targetPath: String = ""): Future[Unit] = {
    implicit val system: ActorSystem = ActorSystem("HttpDownloadClient")

    val downloadUrl = s"http://10.0.89.38:8088/downloadPackage?id=$functionId"
    val outputFilePath = targetPath + s"\\$functionId.jar" // 下载文件保存路径

    println(s"准备从 $downloadUrl 下载文件到 $outputFilePath")

    // 创建 HTTP GET 请求
    val request = HttpRequest(
      method = HttpMethods.GET,
      uri = downloadUrl
    )

    // 发送请求并处理响应
    Http().singleRequest(request).flatMap { response =>
      if (response.status.isSuccess()) {
        println(s"成功接收到响应: ${response.status}")
        val outputFile = new File(outputFilePath)
        val fileSink = FileIO.toPath(outputFile.toPath)

        response.entity.dataBytes.runWith(fileSink).map(_ => ()).andThen { // map to Unit, andThen for side effects
          case Success(_) => println(s"文件下载完成: ${outputFile.getAbsolutePath}")
          case Failure(ex) =>
            println(s"文件写入失败: ${ex.getMessage}")
            response.discardEntityBytes() // 确保丢弃未消费的实体字节
        }
      } else {
        response.entity.toStrict(5.seconds).map(_.data.utf8String).flatMap { body =>
          Future.failed(new RuntimeException(s"下载失败，状态码: ${response.status}, 响应体: $body"))
        }.recoverWith {
          case ex: Exception => Future.failed(new RuntimeException(s"下载失败，状态码: ${response.status}, 无法获取响应体: ${ex.getMessage}", ex))
        }
      }
    }
  }

}
