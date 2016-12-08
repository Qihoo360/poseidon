package InvertedIndex.plugin;

import InvertedIndex.LogParser;
import InvertedIndex.plugin.Function.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.InflaterInputStream;

//TODO

public class CommonLogParser extends LogParser {

    protected int debug_ = 0;

    protected enum Format {
        JSON,
        TAB,
        KV,
    }

    Format format_;
    Vector<TokenParser> tokenParsers_ = new Vector<TokenParser>();
    Map<String, TokenParser> tokenParserMaps_ = new HashMap<>();
    Map<String, String> aliasNames_ = new HashMap<>();
    Set<String> fields_ = new HashSet<>();
    Map<String, Integer> tabFields_ = new HashMap<>();
    Map<String, TokenFilter> tokenfilters_ = new HashMap<>();
    Map<String, String> HdpCacheFiles_;//file_name , path
    Pattern pattern_;

    public CommonLogParser(Configuration conf, String json_str, boolean load_tokenizer) {
        debug_ = 0;
        try {
            //System.err.println(json_str);
            JSONObject json = new JSONObject(json_str);

            String data_format = json.getString("data_format");
            System.err.println("index formart " + data_format);
            loadAliasNames(json);
            if (data_format.compareTo("JSON") == 0) {
                format_ = Format.JSON;
            } else if (data_format.compareTo("KV") == 0) {
                format_ = Format.KV;
                pattern_ = Pattern.compile("<(?<key>[\\w]+)=(?<val>[^>]*)>");
            } else {
                format_ = Format.TAB;
                loadFieldNames(json);
            }

            loadFilterTokenFiles(json, conf, load_tokenizer);
            loadTokenizer(json, load_tokenizer);
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ParseLine(String line, long docid, int line_offset,
                          Mapper<LongWritable, Text, Text, Text>.Context context) {
        boolean ok = false;
        Set<String> output_set = null;
        if (format_ == Format.JSON) {
            try {
                JSONObject json = new JSONObject(line);

                for (int i = 0; i < this.tokenParsers_.size(); i++) {
                    output_set = tokenParsers_.get(i).Process(json);
                    if (output_set == null || output_set.isEmpty()) {
                        continue;
                    }
                    for (String s : output_set) {
                        ok = true;
                        output(s, tokenParsers_.get(i).alias(), docid, line_offset, context);
                    }
                }
            } catch (Exception e) {
                //e.printStackTrace();
                System.err.println(line);
            }
        } else if (format_ == Format.KV) {
            Matcher m = pattern_.matcher(line);
            while (m.find()) {
                if (tokenParserMaps_.containsKey(m.group(1))) {
                    tokenParserMaps_.get(m.group(1)).Process(m.group(2));
                }
            }
        } else {
            String[] vals = line.split("\t");
            for (int i = 0; i < this.tokenParsers_.size(); i++) {
                output_set = tokenParsers_.get(i).Process(vals);
                if (output_set == null || output_set.isEmpty()) {
                    continue;
                }
                for (String s : output_set) {
                    ok = true;
                    output(s, tokenParsers_.get(i).alias(), docid, line_offset, context);
                }
            }
        }

        if (debug_ == 2 && !ok) {
            System.err.println("[DEBUG][ERR]: " + line);
        }
    }

    public Set<String> fields() {
        return fields_;
    }

    public String Filterfiles() {
        String res = new String();
        Iterator it = tokenfilters_.keySet().iterator();
        while (it.hasNext()) {
            if (!res.isEmpty()) {
                res += ",";
            }
            res += (String) it.next();
        }
        System.err.println("filterfiles: " + res);
        return res;
    }

    public int set_debug(int debug) {
        debug_ = debug;
        return debug_;
    }

    protected void output(String str, String flag, long docid, int line_offset,
                          Mapper<LongWritable, Text, Text, Text>.Context context) {
        if (str == null) {
            return;
        }
        if (str.isEmpty()) {
            return;

        }
        if (debug_ == 0) {
            Write(str, flag, docid, line_offset, context);
        } else if (debug_ == 1) {
            System.out.println(str + ": " + flag + "");
        } else {
            try {
                context.write(new Text(str), new Text(flag));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

     /* private static void decodeJSONObject(JSONObject json){
        Iterator<String> keys=json.keys();
        JSONObject jo=null;
        JSONArray ja=null;
        Object o;
        String key;
        while(keys.hasNext()){
            key=keys.next();
            try {
                o=json.get(key);
                if(o instanceof JSONArray){
                    ja = (JSONArray)o;
                    for (int i=0;i<ja.length();i++){
                        decodeJSONObject(ja.getJSONObject(i));
                    }
                }
                else if(o instanceof JSONObject){
                    jo=(JSONObject)o;
                    if(jo.length() >0){
                        decodeJSONObject(jo);
                    }else{
                        System.err.println("got a key:" + key + " is null");
                    }
                }else{
                    System.err.println(key + "---->" + o);
                }
            } catch (JSONException e) {
            }
        }
    }*/

    private Vector<TokenParser> loadTokenizer(JSONObject json, boolean load_tokenizer) {//load_tokenizer确保第二次初始化本类时调用Add,第一次不加载对应函数
        Vector<TokenParser> vector = new Vector<TokenParser>();
        try {
            JSONObject tokenizer = json.getJSONObject("tokenizer");
            Iterator keys = tokenizer.keys();
            JSONObject jo = null;
            Object o;
            Object value;
            String key;
            while (keys.hasNext()) {
                o = keys.next();
                key = o.toString();
                value = tokenizer.get(key);
                TokenParser tokenParser = new TokenParser(this, key);
                if (value instanceof String) {
                    if (load_tokenizer) {
                        tokenParser.Add(value.toString(), null);
                    }
                } else if (value instanceof JSONArray) {
                    JSONArray ja = (JSONArray) value;
                    for (int i = 0; i < ja.length(); i++) {
                        Object ja_o = ja.get(i);
                        if (ja_o instanceof String) {
                            String func = ja_o.toString();
                            if (load_tokenizer) {
                                tokenParser.Add(func, null);
                            }
                        } else if (ja_o instanceof JSONObject) {
                            JSONObject js = (JSONObject) ja_o;
                            String func = js.keys().next().toString();
                            String para = js.getString(func);
                            if (load_tokenizer) {
                                tokenParser.Add(func, para);
                            }
                        }
                    }
                }

                if (aliasNames_.containsKey(tokenParser.key())) {
                    tokenParser.set_alias(aliasNames_.get(tokenParser.key()));
                }

                if (format_ == Format.TAB) {
                    int index = getTabFieldNum(tokenParser.key());
                    if (index == -1) {
                        System.err.println("index not find:" + tokenParser.key());
                        System.exit(-1);
                    }
                    tokenParser.set_index(index);
                }

                tokenParsers_.add(tokenParser);
                tokenParserMaps_.put(tokenParser.key(), tokenParser);
                fields_.add(tokenParser.alias());
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return vector;
    }

    private void loadAliasNames(JSONObject json) {
        try {
            JSONObject alias = json.getJSONObject("alias");
            Iterator keys = alias.keys();
            JSONObject jo = null;

            Object o;
            String value;
            String key;
            while (keys.hasNext()) {
                o = keys.next();
                key = o.toString();
                value = alias.getString(key);
                System.err.println("alias: " + key + " " + value);
                aliasNames_.put(key, value);
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    private void loadFieldNames(JSONObject json) {
        try {
            JSONArray ja = json.getJSONArray("field_names");
            System.err.println("tab find field_names size " + ja.length());
            for (int i = 0; i < ja.length(); i++) {
                System.err.println("tab field " + ja.getString(i) + " " + i);
                tabFields_.put(ja.getString(i), i);
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
    }

    //Done
    //加载分词的过滤文件
    /*
     "token_filter_files" :{
                "file_name1":1000,
                "file_name2":100000
                }

     定位到配置的token_filter_files,
     file_name1,2表示要过滤的分词列表
     1000,10000表示过滤的精度
    **/
    private void loadFilterTokenFiles(JSONObject json, Configuration conf, boolean load_tokenizer) {
        //TODO
        try {
            JSONObject token_filter_files = json.getJSONObject("token_filter_files");
            Iterator keys = token_filter_files.keys();
            JSONObject jo = null;

            Object o;
            int value;
            String key;
            while (keys.hasNext()) {
                o = keys.next();
                key = o.toString();
                value = token_filter_files.getInt(key);
                System.err.println("token_filter_files: " + key + " " + value);

                if (load_tokenizer) {
                    String file_path = null;
                    Path[] cacheFiles = null;
                    try {
                        cacheFiles = DistributedCache.getLocalCacheFiles(conf);
                    } catch (IOException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }
                    for (int i = 0; i < cacheFiles.length; ++i) {
                        String path = cacheFiles[i].toString();
                        System.err.println(path);
                        if (file_path == null && path.indexOf(key) != -1) {
                            file_path = path;
                        }
                    }
                    TokenFilter tokenfilter;
                    if (file_path == null) {
                        System.err.println("not found token filter file " + key + ", maybe a Jutil test");
                        tokenfilter = new TokenFilter(key, value);
                    } else {
                        System.err.println("found token filter file " + key + "hadoop path: " + file_path);
                        tokenfilter = new TokenFilter(file_path, value);
                    }
                    tokenfilters_.put(key, tokenfilter);
                } else {
                    tokenfilters_.put(key, null);
                }
            }
        } catch (JSONException e) {
            e.printStackTrace();
            System.err.printf("the JSONException will be ignore\n");
        }
    }

    //TODO
    public TokenFilter GetFilter(String filtername) {
        return tokenfilters_.get(filtername);
    }

    private int getTabFieldNum(String fields) {
        if (tabFields_.containsKey(fields)) {
            return tabFields_.get(fields);
        }
        return -1;
    }

    public static void decompressFile(InputStream is, OutputStream os) {
        InflaterInputStream iis = new InflaterInputStream(is);
        try {
            int i = 1024;
            byte[] buf = new byte[i];

            while ((i = iis.read(buf, 0, i)) > 0) {
                os.write(buf, 0, i);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] argv) {

    }

}