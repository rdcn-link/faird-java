package link.rdcn.user.exception;

import io.grpc.Metadata;

public enum ErrorCode {
    // 通用错误码
    INTERNAL_SERVER_ERROR(500, "系统内部错误"),
    INVALID_PARAMETER(400, "参数不合法"),
    RESOURCE_NOT_FOUND(404, "资源不存在"),

    // 登录验证错误码
    USER_NOT_FOUND(100, "User not found"),
    INVALID_CREDENTIALS(101, "Invalid username or password"),
    TOKEN_EXPIRED(102, "Token expired");

    private final int code;
    private final String message;


    ErrorCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
