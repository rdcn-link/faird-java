package link.rdcn.user.exception;

import io.grpc.Status;
import org.apache.arrow.flight.FlightRuntimeException;


public enum ErrorCode {
    USER_NOT_FOUND(100, "User not found"),
    INVALID_CREDENTIALS(101, "Invalid credentials"),
    TOKEN_EXPIRED(102, "Token expired"),

    DATAFRAME_NOT_EXIST(201, "DataFrame not exist"),
    LOGIN_REQURIED(202, "Login required"),


    NO_SUCH_ERROR(901, "No such error"),

    SERVER_ADDRESS_ALREADY_IN_USE(301, "Server address already in use"),
    SERVER_ALREADY_STARTED(302, "Server already started");

    private final int value;
    private final String message;
    public final static String INVALID_ERROR_CODE = "-1";

    ErrorCode(int value, String message) {
        this.value = value;
        this.message = message;
    }

    public int getCode() {
        return value;
    }

    public String getMessage() {
        return message;
    }

    public static ErrorCode fromErrorCode(int errorCode) {
        for (ErrorCode code : ErrorCode.values()) {
            if (code.getCode() == errorCode) {
                return code;
            }
        }
        throw new IllegalArgumentException(String.valueOf(errorCode));
    }

    public static ErrorCode fromException(FlightRuntimeException e) {
        String errorCodeStr = e.status().metadata().get("error-code");
        int errorCode = Integer.parseInt(errorCodeStr);
        return fromErrorCode(errorCode);
    }

    public static ErrorCode fromException(Exception e) {
        return NO_SUCH_ERROR;
    }

}