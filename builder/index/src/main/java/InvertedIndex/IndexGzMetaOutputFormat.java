package InvertedIndex;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class IndexGzMetaOutputFormat<K, V> extends TextOutputFormat<K, V> {
    private String folderName;

    //SequenceFileAsBinaryOutputFormat aa = null;
    private class MultipleFilesRecordWriter extends RecordWriter<K, V> {
        private Map<String, RecordWriter<K, V>> fileNameToWriter;
        private FolderNameExtractor<K, V> fileNameExtractor;
        private TaskAttemptContext job;

        public MultipleFilesRecordWriter(FolderNameExtractor<K, V> fileNameExtractor, TaskAttemptContext job) {
            fileNameToWriter = new HashMap<String, RecordWriter<K, V>>();
            this.fileNameExtractor = fileNameExtractor;
            this.job = job;
        }

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            String fileName = fileNameExtractor.extractFolderName(key, value);
            RecordWriter<K, V> writer = fileNameToWriter.get(fileName);
            //System.err.println("Write " + folderName);

            if (writer == null) {
                writer = createNewWriter(fileName, fileNameToWriter, job);
                if (writer == null) {
                    throw new IOException("Unable to create writer for path: " + fileName);
                }
            }
            writer.write(key, value);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            //Map<String, RecordWriter<K, V>> fileNameToWriter;
            Iterator<String> iter = fileNameToWriter.keySet().iterator();
            while (iter.hasNext()) {
                String key = iter.next();
                fileNameToWriter.get(key).close(context);
            }

        }
    }

    private synchronized RecordWriter<K, V> createNewWriter(String folderName,
                                                            Map<String, RecordWriter<K, V>> fileNameToWriter, TaskAttemptContext job) {
        //System.err.println("create " + folderName);
        try {
            //String prefix = "part-";
            Path outputDir = FileOutputFormat.getOutputPath(job);
            String subfix = job.getTaskAttemptID().getTaskID().toString();
            Path path = new Path(outputDir.toString() + "/" + folderName + "/part-" + subfix.substring(subfix.length() - 5, subfix.length()) + ".gz");

            FSDataOutputStream outputStream = null;
            if (path.getFileSystem(job.getConfiguration()).isFile(path)) {
                path.getFileSystem(job.getConfiguration()).delete(path, false);
            }
            outputStream = path.getFileSystem(job.getConfiguration()).create(path);

            //if (job.getConfiguration())
            IndexGzMetaRecordWriter writer = null;
            if (folderName.compareTo("middle") != 0 && folderName.indexOf("index") != -1) {
                writer = new IndexGzMetaRecordWriter(outputStream);
            } else {
                Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
                CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job.getConfiguration());
                //输入和输出均为hdfs路径

                // 创建压缩输出流
                CompressionOutputStream out = codec.createOutputStream(outputStream);
                writer = new IndexGzMetaRecordWriter(out);
            }
            this.folderName = null;
            fileNameToWriter.put(folderName, writer);
            return writer;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public class IndexGzMetaRecordWriter extends RecordWriter<K, V> {

        private OutputStream outputStream = null;

        public IndexGzMetaRecordWriter(OutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public void write(K key, V value) throws IOException,
                InterruptedException {
            //System.err.println("output haha");
            BytesWritable a = (BytesWritable) value;
            //a.write(outputStream);
            //this.outputStream.write(StrToBinstr(value.toString()).getBytes());
            this.outputStream.write(a.getBytes());
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException,
                InterruptedException {
            this.outputStream.close();
        }

        private String StrToBinstr(String str) {
            char[] strChar = str.toCharArray();
            String result = "";
            for (int i = 0; i < strChar.length; i++) {
                result += Integer.toBinaryString(strChar[i]);
            }
            return result;
        }
    }

    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        Path path = super.getDefaultWorkFile(context, extension);
        if (folderName != null) {
            String newPath = path.getParent().toString() + "/" + folderName + "/" + path.getName();
            path = new Path(newPath);
        }
        return path;
    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new MultipleFilesRecordWriter(getFolderNameExtractor(), job);
    }

    public FolderNameExtractor<K, V> getFolderNameExtractor() {
        return new KeyFolderNameExtractor<K, V>();
    }

    public interface FolderNameExtractor<K, V> {
        public String extractFolderName(K key, V value);
    }

    private static class KeyFolderNameExtractor<K, V> implements FolderNameExtractor<K, V> {
        public String extractFolderName(K key, V value) {
            return key.toString();
        }
    }
}
