
//src: hadoop-0.20.2.1U11\src\mapred\org\apache\hadoop\mapreduce\lib\input\LineRecordReader.java
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Treats keys as offset in file and value as line.
 */
public class NLineRecordReader extends RecordReader<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(NLineRecordReader.class);

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private NLineReader in;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;

    private TaskAttemptContext context = null;
    private Configuration job = null;
    private Path path = null;
    private long length = 0;

    public void initialize(InputSplit genericSplit,
                           TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.job = job;
        this.context = context;
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        this.path = file;
        this.length = split.getLength();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            if (0 == split.getLength() && job.getBoolean("mapred.ignore.badcompress", false)) {
                if (null != context && context instanceof TaskInputOutputContext) {
                    ((TaskInputOutputContext) context).getCounter("Input Counter", "Gzip File length is zero").increment(1);
                }
                if (null != this.path) {
                    LOG.warn("Skip 0-length Zip file: " + this.path.toString());
                }
                in = new NLineReader(fileIn, job);
            } else {
                try {
                    in = new NLineReader(codec.createInputStream(fileIn), job);
                    end = Long.MAX_VALUE;
                } catch (IOException e) {
                    if (isIgnoreBadCompress(job, e)) {
                        in = new NLineReader(fileIn, job);
                        end = start;
                        LOG.warn("Skip Bad Compress File: " + this.path.toString());
                        LOG.warn("initialize line read error", e);
                        ((TaskInputOutputContext) context).getCounter("Input Counter", "Skip Bad Zip File").increment(1);
                        ((TaskInputOutputContext) context).getCounter("Input Counter", "Total Skip Bad Zip Length").increment(this.length);
                    } else {
                        throw e;
                    }
                }
            }
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new NLineReader(fileIn, job);
        }
        if (skipFirstLine) {  // skip first line and re-establish "start".
            start += in.readLine(new Text(), 0,
                    (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
        }
        int newSize = 0;
        while (pos < end) {
            try {
                newSize = in.readLine(value, maxLineLength,
                        Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
                                maxLineLength));
            } catch (Throwable exception) {
                LOG.warn("readLine error", exception);
                if (isIgnoreBadCompress(job, exception)) {
                    if (null != context && context instanceof TaskInputOutputContext) {
                        ((TaskInputOutputContext) context).getCounter("Input Counter", "Skip Bad Zip File").increment(1);
                        ((TaskInputOutputContext) context).getCounter("Input Counter", "Total Skip Bad Zip Length").increment(this.length);
                    }
                    if (null != path) {
                        LOG.warn("Skip Bad Zip file: " + path.toString());
                    }
                    return false;
                } else {
                    throw new IOException(exception);
                }
            }
            if (newSize == 0) {
                break;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }

            // line too long. try again
            LOG.info("Skipped line of size " + newSize + " at pos " +
                    (pos - newSize));
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

    public boolean isIgnoreBadCompress(Configuration job, Throwable exception) {
        if (null != job && job.getBoolean("mapred.ignore.badcompress", false)) {
            String exceptionStr = StringUtils.stringifyException(exception);
            String[] keywordsBL = job.getStrings("mapred.ignore.badcompress.keywords.blacklist", "Could not obtain block");
            if (null != keywordsBL) {
                for (String keyword : keywordsBL) {
                    if (null != keyword && exceptionStr.contains(keyword)) {
                        return false;
                    }
                }
            }

            String[] keywords = job.getStrings("mapred.ignore.badcompress.keywords", "org.apache.hadoop.io.compress.DecompressorStream",
                    "org.apache.hadoop.io.compress.MultiMemberGZIPInputStream",
                    "org.apache.hadoop.io.compress.GzipCodec$GzipInputStream",
                    "com.hadoop.compression.lzo.LzopCodec$LzopInputStream");

            if (null != keywords) {
                for (String keyword : keywords) {
                    if (null != keyword && exceptionStr.contains(keyword)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    //@Override
    public long getPos() {
        return pos;
    }

    /**
     * Get the progress within the split
     */
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    public synchronized void close() throws IOException {
        if (in != null) {
            try {
                in.close();
            } catch (Throwable exception) {
                if (isIgnoreBadCompress(job, exception)) {
                    if (null != path) {
                        LOG.warn("Skip Bad Compress File: " + this.path.toString());
                    }
                    LOG.warn("close error", exception);
                } else {
                    throw new IOException(exception);
                }
            }
        }
    }
}
