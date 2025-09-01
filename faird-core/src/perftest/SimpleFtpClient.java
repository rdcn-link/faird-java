/**
 * @Author Yomi
 * @Description:
 * @Data 2025/8/28 16:42
 * @Modified By:
 */
package org.example;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SimpleFtpClient {

    public static void main(String[] args) {
        // 创建 FTPClient 实例
        FTPClient ftpClient = new FTPClient();

        // FTP 服务器信息
        String server = "10.0.87.114";
        int port = 21; // FTP 服务器的端口号
        String user = "ftpserver"; // 用户名
        String password = "ftpserver"; // 密码，匿名用户通常为空
//        String remoteFilePath = "/home/ftpserver/data/json/million_lines.json"; // 服务器上的文件路径
        String remoteFilePath = "/home/faird/faird/faird-core/src/test/demo/data/csv/data_7.csv";
//        String localFilePath = "/home/ftptest/million_lines_1.json"; // 本地保存路径，请确保该文件夹存在
        String localFilePath = "/home/ftptest/data_1.csv";
//        String processedFilePath = "/home/ftptest/million_lines_2.json";
        String processedFilePath = "/home/ftptest/data_2.csv";
        enableFtpLogging(ftpClient);

        int batchSize = 1000000;

        Function<String, String> jsonOp = line -> {
//            JsonObject jsonObject = JsonParser.parseString(line).getAsJsonObject();
            return line;
        };

        Function<String, String> jsonSelectOp = line -> {
            JsonObject jsonObject = JsonParser.parseString(line).getAsJsonObject();
            // 删除指定的键
            jsonObject.remove("timestamp");
            jsonObject.remove("measurement");
            return jsonObject.toString();
        };

        Function<String, String> csvOp = line -> {
            String[] splits = line.split(",");
            return String.join(",", splits);
        };

        Function<String, String> csvSelectOp = line -> {
            String[] columns = line.split(",");
            int[] columnsToRemove = {0,1};

            List<String> remainingColumns = new ArrayList<>();
            for (int i = 0; i < columns.length; i++) {
                // 检查当前列索引是否在要删除的数组中
                boolean shouldRemove = false;
                for (int col : columnsToRemove) {
                    if (i == col) {
                        shouldRemove = true;
                        break;
                    }
                }
                if (!shouldRemove) {
                    remainingColumns.add(columns[i]);
                }
            }

            // 将剩余的列重新拼接成一行，并写入新文件
            return String.join(",", remainingColumns);
        };

        try {
            // 连接到服务器
            System.out.println("Connecting to server: " + server + " on port " + port);
            ftpClient.connect(server, port);

            // 登录
            if (ftpClient.login(user, password)) {
                System.out.println("Login successful.");
                ftpClient.enterLocalPassiveMode();
                // 检查连接是否成功
                int replyCode = ftpClient.getReplyCode();
                if (org.apache.commons.net.ftp.FTPReply.isPositiveCompletion(replyCode)) {
                    ftpClient.setFileType(FTPClient.ASCII_FILE_TYPE);
                    File localFile = new File(localFilePath);
                    long downloadStartTime = System.currentTimeMillis();
                    try (InputStream inputStream = ftpClient.retrieveFileStream(remoteFilePath);
                         BufferedWriter writer = new BufferedWriter(new FileWriter(processedFilePath));
                         FileOutputStream fos = new FileOutputStream(localFile);
                         FileInputStream fis = new FileInputStream(localFile);) {
                        System.out.println("Downloading file: " + remoteFilePath);
                        processFtpStream(inputStream, writer, csvSelectOp, batchSize);
                        boolean success = ftpClient.completePendingCommand();

                        if (success) {
                            System.out.println("File downloaded successfully to: " + localFilePath);
                        } else {
                            System.out.println("File download failed. Check file path or permissions.");
                            System.out.println("Server reply: " + ftpClient.getReplyString());
                        }
                    }
                }
            } else {
                System.out.println("Login failed.");
            }

        } catch (IOException e) {
            System.err.println("Error connecting to FTP server: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 确保断开连接
            try {
                if (ftpClient.isConnected()) {
                    ftpClient.logout();
                    ftpClient.disconnect();
                    System.out.println("Disconnected from server.");
                }
            } catch (IOException e) {
                System.err.println("Error during disconnection: " + e.getMessage());
            }
        }
    }

    /**
     * 读取从FTP服务器获取的输入流，逐行处理JSON对象，并记录每秒传输行数。
     *
     * @param inputStream 从FTP服务器获取的文件输入流
     * @param writer      处理后保存的新文件写入器
     * @throws IOException
     */
    private static void processFtpStream(InputStream inputStream, BufferedWriter writer, Function<String, String> op, int batchSize) throws IOException {
        long linesProcessed = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            Long end = System.currentTimeMillis();
            Long start = end;
            List<String> batch = new ArrayList<>(batchSize);
            while ((line = reader.readLine()) != null) {
                // 跳过空行
                if (line.trim().isEmpty()) {
                    continue;
                }
                batch.add(line);
                try {
                    // 将每行解析为JSON对象
                    // 将处理后的JSON对象写回新文件

                    linesProcessed++;

                    if (batch.size() == batchSize) {
                        processAndWriteBatch(batch, op, writer);
                        batch.clear(); // 清空批次列表
                        start = end;
                        end = System.currentTimeMillis();
                        double duration = (end - start);
                        System.out.printf("Processed %d rows in %.2f s. Average speed: %.2f rows/sec.\n", batchSize, duration / 1000.0, batchSize / (duration / 1000.0));
                    }
                } catch (Exception e) {
                    System.err.println("Error parsing JSON on line: " + line);
                    // 可以选择跳过或处理这个错误
                }
            }
        }
        writer.close();
        long endTime = System.currentTimeMillis();
        long totalDuration = endTime - startTime;
        System.out.printf("Total processing finished. Processed %d lines in %d ms.\n", linesProcessed, totalDuration);
        if (totalDuration > 0) {
            System.out.printf("Overall average speed: %.2f lines/sec.\n", (double) linesProcessed / (totalDuration / 1000.0));
        }
    }

    private static void processAndWriteBatch(
            List<String> batch,
            Function<String, String> op,
            BufferedWriter writer) throws IOException {

        // 使用 Stream API 高效地对批次进行处理和拼接
        String processedBatch = batch.stream()
                .map(line -> {
                    try {
                        return op.apply(line);
                    } catch (Exception e) {
                        // 可以在这里处理解析错误，例如返回空字符串或特定标记
                        System.err.println("Error applying function to line: " + line);
                        return null; // 或者返回一个错误标记
                    }
                })
                .filter(line -> line != null) // 过滤掉处理失败的行
                .collect(Collectors.joining(System.lineSeparator()));

    }

    private static void enableFtpLogging(FTPClient ftpClient) {
        // 将命令和响应打印到标准输出
        ftpClient.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out), true));
    }
}