package link.rdcn.util

import link.rdcn.ErrorCode
import org.apache.arrow.flight.FlightRuntimeException

import java.io.IOException
import java.net.ConnectException
import scala.util.{Failure, Success, Try}

/**
 * Handles conversion between various exception types and ServerException
 * with proper ErrorCode mapping.
 */
class ExceptionHandler private()

object ExceptionHandler {

  /**
   * get ErrorCode from any input
   */
  def getErrorCode(input: Any): ErrorCode = input match {
    // 处理 FlightRuntimeException
    case flightEx: FlightRuntimeException =>
      if (flightEx.getCause.isInstanceOf[ConnectException])
        ErrorCode.SERVER_NOT_RUNNING
      else Try(flightEx.status().metadata()) match {
        case Success(metadata) =>
          Option(metadata.get("error-code"))
            .flatMap(s => Try(s.toInt).toOption)
            .flatMap(code => Try(getErrorCode(code)).toOption)
            .getOrElse(ErrorCode.NO_SUCH_ERROR)
        case Failure(_) => ErrorCode.ERROR_CODE_NOT_EXIST
      }

    // 处理 IllegalStateException
    case _: IllegalStateException => ErrorCode.SERVER_ALREADY_STARTED

    // 处理 IOException
    case e: IOException => {
      e.getCause match {
        case _: ConnectException => ErrorCode.SERVER_NOT_RUNNING
        case _ => ErrorCode.SERVER_ADDRESS_ALREADY_IN_USE
      }
    }


    // 处理 错误编号
    case code: Int =>
      ErrorCode.values.find(_.code == code)
        .getOrElse(throw new IllegalArgumentException(code.toString))

    // 其他 Exception 或未知输入
    case e: Exception => { e.getCause match {
      case _: ConnectException => ErrorCode.SERVER_NOT_RUNNING
      case _ => ErrorCode.NO_SUCH_ERROR
    }

    }
    case _ =>
      throw new IllegalArgumentException(s"Unsupported input type: ${input.getClass.getName}")
  }
}
