import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URLDecoder;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import java.lang.Integer;
import java.lang.NumberFormatException;

public class Reader {

    static private final Logger logger = LoggerFactory.getLogger(Reader.class);

    public static void main(String[] args) throws Exception {
        int port = 9997;
        if(args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch(NumberFormatException e) {
                System.err.println("bad port");
                System.exit(1);
            }
        }
        StartHttpService(port);
    }

    public static void StartHttpService(int port) {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/read-hdfs", new MyHandler());
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            logger.info("StartHttpService start, port: {}", port);
        } catch (IOException e) {
            logger.warn("StartHttpService failed");
        }
    }

    public static String Decode(String encodedStr) {
        try {
            return URLDecoder.decode(encodedStr, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            logger.warn("Decode not support UTF-8");
            return encodedStr;
        }
    }

    public static Map<String, String> QueryToMap(String query) {
        Map<String, String> result = new HashMap<String, String>();
        for (String param : query.split("&")) {
            String pair[] = param.split("=");
            if (pair.length > 1) {
                result.put(Decode(pair[0]), Decode(pair[1]));
            } else if (pair.length > 0) {
                result.put(Decode(pair[0]), "");
            }
        }
        return result;
    }

    public static void ReportError(Exception exception, OutputStream out) {
        try {
            out.write(exception.getMessage().getBytes());
            exception.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            logger.warn("ReportError throw, msg: {}, out: {}", exception.getMessage(), out);
        }
    }

    static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) {
            logger.info("handle begin, uri: {}, remote: {}, local: {}",
                new Object[] {t.getRequestURI().getQuery(), t.getRemoteAddress(), t.getLocalAddress()});
            long begin = System.currentTimeMillis();
            OutputStream out = null;
            try {
                t.sendResponseHeaders(200, 0);
                out = t.getResponseBody();
            } catch (IOException e) {
                logger.error("handle service down, message: {}, uri: {}",
                            e.getMessage(), t.getRequestURI().getQuery());
                IOUtils.closeStream(out);
                t.close();
                return;
            }

            try {
                Map<String, String> params = QueryToMap(t.getRequestURI().getRawQuery());
                String path = params.get("path");
                long offset = Long.parseLong(params.get("offset"));
                int length = Integer.parseInt(params.get("length"), 10);

                CopyPartially(path, out, offset, length);
            } catch (IOException e) {
                logger.warn("CopyPartially throw, message: {}, uri: {}", e.getMessage(), t.getRequestURI().getQuery());
                ReportError(e, out);
            } catch (NumberFormatException e) {
                logger.warn("handle bad param, message: {}, uri: {}", e.getMessage(), t.getRequestURI().getQuery());
                ReportError(new Exception("offset and length must be number"), out);
            } catch (OutOfMemoryError e) {
                System.err.println("out of memory");
                System.exit(1);
            } finally {
                IOUtils.closeStream(out);
                t.close();
                logger.info("handle time cost: {}", System.currentTimeMillis() - begin);
            }
        }
    }

    public static void CopyPartially(String pathStr, OutputStream out, long offset, int length) throws IOException {
        logger.info("CopyPartially begin, path: {}, offset: {}, length: {}", new Object[] {pathStr, offset, length});
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);

        try(FileSystem fs = FileSystem.get(path.toUri(), conf)) {
            logger.debug("begin exists, path: {}", pathStr);
            if (!fs.exists(path)) {
                logger.warn("CopyPartially File does not exist, path: {}", pathStr);
                out.write(("File does not exist: " + pathStr).getBytes());
                return;
            }
            logger.debug("begin getFileStatus, path: {}", pathStr);
            FileStatus stat = fs.getFileStatus(path);
            if (stat.isDir()) {
                logger.warn("CopyPartially Can not read directory, path: {}", pathStr);
                out.write(("Can not read directory: " + pathStr).getBytes());
                return;
            }
            if (offset + length > stat.getLen()) {
                logger.warn("CopyPartially Not enough data, path: {}", pathStr);
                out.write(("Not enough data: " + pathStr).getBytes());
                return;
            }

            logger.debug("begin read, path: {}", pathStr);
            try(FSDataInputStream in = fs.open(path)) {
                int bufferSize = 48 * 1024;
                byte[] buffer = new byte[bufferSize];
                int readSize = 0;
                for (long pos = offset; pos < offset + length; pos += readSize) {
                    if (offset + length - pos < bufferSize) {
                        bufferSize = (int)(offset + length - pos);
                    }
                    long begin = System.currentTimeMillis();
                    readSize = in.read(pos, buffer, 0, bufferSize);
                    logger.debug("CopyPartially read time cost: {}, bufferSize: {}",
                        System.currentTimeMillis() - begin, bufferSize);
                    if (readSize == -1) {
                        logger.warn("CopyPartially EOF path: {}, pos: {}, totalLength: {}",
                                    new Object[] {path, pos, stat.getLen()});
                        return;
                    }
                    if (readSize != bufferSize) {
                        logger.warn("CopyPartially readSize: {}, bufferSize: {}", readSize, bufferSize);
                    }
                    out.write(buffer, 0, readSize);
                }
            }
        }
    }
}
