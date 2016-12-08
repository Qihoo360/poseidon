package InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by liwei on 8/31/16.
 */
public class HDFSFileWriter {


    public static void UploadFile(String localFile, final String dstHdfsFile) {
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(localFile));
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(localFile), conf);
            if (fs.exists(new Path(dstHdfsFile))) {
                System.err.println(dstHdfsFile + " is exists");
                in.close();
                return;
            }
            OutputStream out = fs.create(new Path(dstHdfsFile), new Progressable() {
                        @Override
                        public void progress() {
                            System.err.println("uploading " + dstHdfsFile);
                        }
                    }
            );

            IOUtils.copyBytes(in, out, 4096, true);
            fs.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }

    public static String ReadLine(FSDataInputStream in) {
        String line = "";
        try {
            line = in.readLine();
            /*
            byte[] buf= new byte[40960];
            int len = in.read(buf);
            String str = new String(buf);
            int pos = str.indexOf('\n');
            if (pos == -1) {
                return line;
            }
            line =  str.substring(0, pos);*/
        } catch (Exception e) {

        }
        return line;
    }

    public static void CreateFirstDocidList(String dstDoc, String firstDoc) {
        FSDataOutputStream out = null;
        try {
            Path path = new Path(dstDoc);
            Path firtIdPath = new Path(firstDoc);

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            System.err.println(dstDoc);
            System.err.println(firstDoc);

            FileStatus[] status = fs.listStatus(path);
            Path[] listPath = FileUtil.stat2Paths(status);
            out = fs.create(firtIdPath);
            for (Path p : listPath) {
                System.err.println("find doc: " + p);


                FSDataInputStream in = fs.open(p);
                String line = ReadLine(in);
                String[] docs = line.split("\t");
                if (line.isEmpty()) {
                    System.err.println("find doc: " + p + " get empty docid");
                } else {
                    System.err.println("find doc: " + p + " " + line);
                    String newLine = p.getName() + "\t" + docs[0] + "\n";
                    System.err.println("write doc: " + p + " " + newLine);
                    out.write(newLine.getBytes());
                }
                in.close();
            }
            out.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
        }


    }

}
