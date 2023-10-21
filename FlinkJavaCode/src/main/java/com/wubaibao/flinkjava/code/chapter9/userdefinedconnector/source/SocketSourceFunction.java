package com.wubaibao.flinkjava.code.chapter9.userdefinedconnector.source;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

public class SocketSourceFunction extends RichSourceFunction<RowData> {

    private final String hostname;
    private final int port;
    private final byte byteDelimiter;

    private volatile boolean isRunning = true;
    private Socket currentSocket;

    public SocketSourceFunction(String hostname, int port, byte byteDelimiter) {
        this.hostname = hostname;
        this.port = port;
        this.byteDelimiter = byteDelimiter;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (isRunning) {
            // 持续从 socket 消费数据
            try (final Socket socket = new Socket()) {
                // 保存当前 socket，以便在 cancel 时关闭
                currentSocket = socket;
                // 连接 socket
                socket.connect(new InetSocketAddress(hostname, port), 0);
                // 读取数据
                try (InputStream stream = socket.getInputStream()) {
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int b;
                    while ((b = stream.read()) >= 0) {
                        // 持续写入 buffer 直到遇到分隔符
                        if (b != byteDelimiter) {
                            buffer.write(b);
                        }
                        // 解码并处理记录
                        else {
                            //将buffer 按照逗号分割符切分，组成RowData
                            String value = buffer.toString();

                            GenericRowData genericRowData = new GenericRowData(6);
                            genericRowData.setField(0, StringData.fromString(value.split(",")[0]));
                            genericRowData.setField(1, StringData.fromString(value.split(",")[1]));
                            genericRowData.setField(2, StringData.fromString(value.split(",")[2]));
                            genericRowData.setField(3, StringData.fromString(value.split(",")[3]));
                            genericRowData.setField(4, Long.valueOf(value.split(",")[4]));
                            genericRowData.setField(5, Long.valueOf(value.split(",")[5]));

                            ctx.collect(genericRowData);

                            buffer.reset();
                        }
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace(); // 打印并继续
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            currentSocket.close();
        } catch (Throwable t) {
            // 忽略
        }

    }
}
