### Faird 使用TLS证书


1. 使用 Openssl 创建证书

~~~
# 生成根私钥
openssl genrsa -out rootCA.key 4096
 
# 生成根证书（有效期365天）
openssl req -x509 -new -nodes -key rootCA.key -sha256 -days 365 -out rootCA.pem

# 生成服务器私钥
openssl genrsa -out server.key 2048
 
# 创建一个服务器证书签名请求（服务器域名或IP地址）
openssl req -new -key server.key -out server.csr -subj "/CN=localhost"
 
# 使用根证书签发服务器证书
openssl x509 -req -in server.csr -CA rootCA.pem -CAkey rootCA.key -CAcreateserial -out server.crt -days 365 -sha256
~~~

1.1. 使用生成 server.crt 和 server.key 配置 faird

1.2. 客户端使用时需导入 server.crt 证书

~~~
# java 客户端使用前导入
keytool -import -alias mycert -file server.crt -keystore $JAVA_HOME/jre/lib/security/cacerts -storepass changeit

# 使用工具 portecle-1.11 导入，然后导出证书库文件后，使用环境变量引入证书库
System.setProperty("javax.net.ssl.trustStore", "D:\\workspace\\faird-java\\src\\main\\resources\\conf\\faird");
~~~

2. 使用 java keytool 创建证书

~~~
# 创建证书
keytool -genkeypair -alias server -keyalg RSA -keysize 2048 -keystore server.keystore.jks -validity 3650 -storetype PKCS12 -storepass changeit -keypass changeit -dname "CN=localhost, OU=Example, O=Example, L=City, S=State, C=Country"

# 导出证书
keytool -export -alias server -keystore server.keystore.jks -rfc -file server.pem
~~~

2.1. 使用生成 server.pem 配置 faird

~~~
服务端配置
grpc:
  server:
    ssl_certificate_chain_file: /path/to/grpcserver.pem
    private_key_file: /path/to/grpcserver.keystore.jks
    ssl_force_client_auth: true  # 根据需要配置客户端认证

客户端配置
grpc:
  client:
    ssl_target_name_override: localhost  # 根据需要配置
    secure: true
    trust_cert_collection: /path/to/client.truststore.jks
~~~