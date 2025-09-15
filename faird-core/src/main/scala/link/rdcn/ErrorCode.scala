package link.rdcn

import enumeratum._

import scala.collection.immutable

sealed abstract class ErrorCode(val code: Int,
                                val message: String) extends EnumEntry {
  val INVALID_ERROR_CODE = "-1"
}

// TODO: Remove this ErrorCode enum. Use NewErrorCode instead.
case object ErrorCode extends Enum[ErrorCode] {
  val values: immutable.IndexedSeq[ErrorCode] = findValues

  case object USER_NOT_FOUND extends ErrorCode(100, "User not found")

  case object INVALID_CREDENTIALS extends ErrorCode(101, "Invalid credentials")

  case object TOKEN_EXPIRED extends ErrorCode(102, "Token expired")

  case object DATAFRAME_NOT_EXIST extends ErrorCode(201, "DataFrame not exist")

  case object DATAFRAME_ACCESS_DENIED extends ErrorCode(202, "DataFrame access denied")

  case object USER_NOT_LOGGED_IN extends ErrorCode(203, "User not logged in")

  case object SERVER_NOT_RUNNING extends ErrorCode(301, "Server not running")

  case object UNKNOWN_ERROR extends ErrorCode(901, "Unknown error")

  case object ERROR_CODE_NOT_EXIST extends ErrorCode(902, "Error code not exist")

  case object SERVER_ADDRESS_ALREADY_IN_USE extends ErrorCode(301, "Server address already in use")

  case object SERVER_ALREADY_STARTED extends ErrorCode(302, "Server already started")

}
