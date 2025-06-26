package link.rdcn.user.exception;

import com.google.common.base.Preconditions;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;


//import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.protobuf.StatusProto;
import org.apache.arrow.flight.perf.impl.PerfOuterClass;

/**
 * @Author renhao
 * @Description:
 * @Data 2025/6/24 11:06
 * @Modified By:
 */
public class InvalidCredentialsException extends AuthException {
    private static final io.grpc.Status status = io.grpc.Status.NOT_FOUND
            .withDescription("无效的用户名/密码!");

    public InvalidCredentialsException() {
        super(status);
    }
}