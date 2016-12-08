package InvertedIndex.plugin;

import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.zip.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.regex.*;


public class Util {
    static Set<String> domains_ = new HashSet<String>();

    static {
        domains_.add("com");
        domains_.add("edu");
        domains_.add("gov");
        domains_.add("net");
        domains_.add("org");
        domains_.add("cn");
        domains_.add("hk");

    }

    public static final int BUFFER = 1024;


    /*
     * 字符串中是否包含特殊字符
        String str = decoderStr("hDGkN4QxpDeBMOk3", false);
        true:  System.out.println(Util.IsUnReadable(str));
        false: System.out.println(Util.IsUnReadable("weoirus..&*^&*230  ??"));
        false: System.out.println(Util.IsUnReadable("蠙~搦%"));
        false: System.out.println(Util.IsUnReadable("a?LＲ--蔔sdfe"));
        false: System.out.println(Util.IsUnReadable("as嘦fa3謪 er"));
        false: System.out.println(Util.IsUnReadable("eddds"));
        false: System.out.println(Util.IsUnReadable("遍历分词数据"));
     * */
    public static boolean IsUnReadable(String str) {
        //System.out.println("len: "+str.length()+", enc: "+enc+", len: "+buf.length);
        String str2 = new String(str.replace('?', ' '));
        try {
            byte[] buf = str2.getBytes("gbk");
            for (byte b : buf)
                if (b == 0x3f /* '?' */)
                    return true;
        } catch (/*UnsupportedEncodingException*/Exception e) {
            return true;
        }
        return false;
    }


    /* *
         test_str(" 324abc123abc75 ");
        test_str(" 876");
        test_str("111");
     * */
    public static boolean IsDigit(String str) {
        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean IsHexadecimal(String str) {
        Pattern p = Pattern.compile("^0x[a-fA-F0-9]+");
        Matcher m = p.matcher(str);
        if (m.matches()) {
            return true;
        }
        return false;
    }

    /*
         IsChinese(" abc ");
        IsChinese("a我");
        IsChinese("我");
        IsChinese("䟇");
        IsChinese("？");
        IsChinese("?");
     * */
    public static boolean IsChinese(String str) {

        /**
         Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
         Matcher m = p.matcher(str);
         if(m.find()){
         return true;
         }
         return false;
         */
        String a = null;
        try {
            a = new String(str.getBytes(), "ASCII");
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (a != null && a.equals(str)) {
            //System.out.println( str+": none zh-cn" );
            return false;
        } else {
            //System.out.println( str+": zh-cn" );
        }
        return true;
    }

    /*
     *  1461839056335
        1461839056339
        1461839056354
         System.out.println(System.currentTimeMillis());
        for( int i=0; i<1000; i++ ) {
            String [] cols = line.split("\t");
        }
        System.out.println(System.currentTimeMillis());
        for( int i=0; i<1000; i++ ) {
            List<String> cols = Util.Split(line, "\t");
        }
        System.out.println(System.currentTimeMillis());
        List<String> cols = Util.Split(line, "\t");
     * */
    public static List<String> Split(String line, String sptr) {

        List<String> strs = null;
        if (line == null || sptr == null || line.isEmpty())
            return strs;

        strs = new ArrayList<String>();
        int pos = line.indexOf(sptr);
        if (sptr.isEmpty() || pos == -1) {
            strs.add(line);
            return strs;
        }

        int begin = 0;
        while (pos != -1) {
            strs.add(line.substring(begin, pos));
            begin = pos + sptr.length();
            pos = line.indexOf(sptr, begin);
        }
        strs.add(line.substring(begin));

        return strs;
    }

    public static Vector<String> ParseIp(String val) {
        Vector<String> res = new Vector();
        String[] ips = val.split("\\.");
        res.add(val);
        if (ips.length == 4) {
            String ip_prefix = ips[0] + "." + ips[1] + "." + ips[2];
            res.add(ip_prefix);
        }
        return res;
    }

    public static Set<String> ParseIpSet(String val) {
        Set<String> res = new HashSet<>();
        String[] ips = val.split("\\.");
        res.add(val);
        if (ips.length == 4) {
            String ip_prefix = ips[0] + "." + ips[1] + "." + ips[2];
            res.add(ip_prefix);
        }
        return res;
    }

    public static Vector<String> ParsePath(String val, Set<String> set, boolean path_combo) {

        Vector<String> res = new Vector();
        String[] fpath_parts = val.split("\\\\");
        res.add(val);
        set.add(val);
        String pre_path = null;
        for (int i = 0; i < fpath_parts.length; i++) {
            if (fpath_parts[i].isEmpty()) {
                pre_path = null;
                continue;
            }
            if (!set.contains(fpath_parts[i])) {
                set.add(fpath_parts[i]);
                res.add(fpath_parts[i]);
            }
            if (path_combo && pre_path != null) {
                String combo_path = pre_path + "\\" + fpath_parts[i];
                if (!set.contains(combo_path)) {
                    set.add(combo_path);
                    res.add(combo_path);
                }
            }
            pre_path = fpath_parts[i];
        }
        return res;
    }

    public static Vector<String> ParsePath(String val, Set<String> set) {
        return ParsePath(val, set, true);
    }

    public static Vector<String> ParsePath(String val) {
        Set<String> set = new HashSet();
        return ParsePath(val, set);
    }

    public static Vector<String> ParseUrl(String val, Set<String> set) {
        Vector<String> vec = new Vector();
        if (val.isEmpty()) {
            return vec;
        }
        if (!set.contains(val)) {
            vec.add(val);
            set.add(val);
        }
        URI uri = null;
        try {
            uri = new URI(val);
            uri = uri.normalize();
        } catch (URISyntaxException e) {
            return vec;
        }
        String host = uri.getHost();
        int port = uri.getPort();
        if (host != null && host != val) {
            if (!set.contains(host)) {
                vec.add(host);
                set.add(host);
            }
        } else {
            host = val;
        }

        if (port > 0) {
            host = host + ":" + port;
            if (!set.contains(host)) {
                vec.add(host);
                set.add(host);
            }
        }
        String[] host_parts = host.split("\\.");
        int len = host_parts.length;
        String part_host = "";
        if (len > 2) {
            part_host = host_parts[len - 1];
        }
        for (int i = len - 2; i > 0; i--) {
            part_host = host_parts[i] + "." + part_host;
            if (i == len - 2 && domains_.contains(host_parts[i])) {
                continue;
            }
            if (!set.contains(part_host)) {
                vec.add(part_host);
                set.add(part_host);
            }
        }
        String path = uri.getPath();

        String path_url = "";
        if (path == null) {
            return vec;
        }
        String[] paths = path.split("/");

        if (paths.length > 1 && !paths[1].isEmpty()) {
            path_url = host + "/" + paths[1];
            if (!path_url.isEmpty()) {
                if (!set.contains(path_url)) {
                    vec.add(path_url);
                    set.add(path_url);
                }
            }
            if (paths.length > 2 && !paths[1].isEmpty() && !paths[2].isEmpty()) {
                String key = paths[1] + "/" + paths[2];
                if (!set.contains(key)) {
                    vec.add(key);
                    set.add(key);
                }
            }
            if (paths.length > 3 && !paths[1].isEmpty() && !paths[2].isEmpty() && !paths[3].isEmpty()) {
                String key = host + "/" + paths[1] + "/" + paths[2] + "/" + paths[3];
                if (!set.contains(key)) {
                    vec.add(key);
                    set.add(key);
                }
            }
        }
        return vec;
    }

    public static Vector<String> ParseUrl(String val) {
        Set<String> set = new HashSet();
        return ParseUrl(val, set);
    }

    public static String Base64DecoderStr(String str, boolean flag) {
        String result = null;

        try {
            byte[] bit = Base64.decodeBase64(str.getBytes());
            result = new String(bit, "UTF-8");
            if (flag == true) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                InflaterOutputStream zos = new InflaterOutputStream(bos);
                zos.write(bit);
                zos.close();
                result = new String(bos.toByteArray(), "UTF-8");
            }
        } catch (Exception err) {
            return str;
        }
        return result;
    }

    public static String Base64EncoderStr(String str, boolean flag) {
        String result = null;

        try {
            byte[] bit = Base64.encodeBase64(str.getBytes());
            result = new String(bit, "UTF-8");
            if (flag == true) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                InflaterOutputStream zos = new InflaterOutputStream(bos);
                zos.write(bit);
                zos.close();
                result = new String(bos.toByteArray(), "UTF-8");
            }
        } catch (Exception err) {
            return str;
        }
        return result;
    }

    public static String Base64DecoderStr(String str) {
        String result = null;
        byte[] bit = Base64.decodeBase64(str.getBytes());
        result = new String(bit);
        return result;
    }

    public static String ReadFile(String Path) {
        BufferedReader reader = null;
        String laststr = "";
        try {
            FileInputStream fileInputStream = new FileInputStream(Path);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            reader = new BufferedReader(inputStreamReader);
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                laststr += tempString;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return laststr;
    }

    public static String Compress(String data) {
        ByteArrayInputStream bais = new ByteArrayInputStream(data.getBytes());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        String plain = "";
        try {
            Compress(bais, baos);
            byte[] output = baos.toByteArray();
            baos.flush();
            baos.close();
            bais.close();
            plain = new String(output);
        } catch (Exception e) {

        }
        return plain;
    }

    /**
     * 数据压缩
     *
     * @param is
     * @param os
     * @throws Exception
     */
    public static void Compress(InputStream is, OutputStream os)
            throws Exception {

        GZIPOutputStream gos = new GZIPOutputStream(os);

        int count;
        byte data[] = new byte[BUFFER];
        while ((count = is.read(data, 0, BUFFER)) != -1) {
            gos.write(data, 0, count);
        }

        gos.finish();

        gos.flush();
        gos.close();
    }


    public static byte[] Gzuncompress(byte[] data) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Inflater deCompressor = new Inflater(false);
        byte[] unCompressed = null;
        try {
            deCompressor.setInput(data);

            while (!deCompressor.finished()) {
                byte[] buf = new byte[512];
                int count = deCompressor.inflate(buf);
                bos.write(buf, 0, count);
            }
            unCompressed = bos.toByteArray();
            bos.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deCompressor.end();
        }
        return unCompressed;
    }

    /* @param stream 要处理的字符串
    * @param trimstr 要去掉的字符串
    * @return 处理后的字符串
    */
    public static String TrimStr(String stream, String trimstr) {
        // null或者空字符串的时候不处理
        if (stream == null || stream.length() == 0 || trimstr == null || trimstr.length() == 0) {
            return stream;
        }

        // 结束位置
        int epos = 0;

        // 正规表达式
        String regpattern = "[" + trimstr + "]*+";
        Pattern pattern = Pattern.compile(regpattern, Pattern.CASE_INSENSITIVE);

        // 去掉结尾的指定字符
        StringBuffer buffer = new StringBuffer(stream).reverse();
        Matcher matcher = pattern.matcher(buffer);
        if (matcher.lookingAt()) {
            epos = matcher.end();
            stream = new StringBuffer(buffer.substring(epos)).reverse().toString();
        }

        // 去掉开头的指定字符
        matcher = pattern.matcher(stream);
        if (matcher.lookingAt()) {
            epos = matcher.end();
            stream = stream.substring(epos);
        }
        // 返回处理后的字符串
        return stream;
    }


    static public String PickupPath(String str, boolean explicit) {

        if (-1 != str.indexOf('\t') || -1 != str.indexOf('\n')) {
            return null;
        }

        int col_pos = str.indexOf(':');
        if (col_pos == 0 || (col_pos == (str.length() - 1)))
            return null;

        int dot_pos = str.indexOf('.');
        // 精确模式下，冒号和点都要有，要更精确的话，可以处理盘符前缀后使用java的path类
        if (explicit && (dot_pos == -1 || col_pos == -1))
            return null;

        if (col_pos != -1) {
            // 有冒号，验证冒号前要是盘符，冒号后要是斜线
            char cr = str.charAt(col_pos - 1);
            if (!(cr >= 'a' && cr <= 'z') && !(cr >= 'A' && cr <= 'Z'))
                return null;
            if (str.charAt(col_pos + 1) != '\\' && str.charAt(col_pos + 1) != '/')
                return null;

            str = str.substring(col_pos - 1);
        } else {
            // 相对路径的
            // 反斜线要多于一个
            int fl = 1;
            int tmp_pos = -1;
            while ((tmp_pos = str.indexOf('\\', tmp_pos + 1)) != -1) {
                fl++;
            }
            if (3 > fl)
                return null;
        }

        // 符号数量占比
        if (dot_pos == -1 || col_pos == -1) {
            float sym_p = Util.SymbolRatio(str);
            if (sym_p > 0.3f)
                return null;
        }

        // 文件后缀暂时没法很好的判断

        // windows下路径中不该存在的符号（代码里是有可能的，如使用printf的pattern，这种现在也是丢掉的）
        if (-1 != str.indexOf('?') || -1 != str.indexOf('*') || -1 != str.indexOf('<')
                || -1 != str.indexOf('>') || -1 != str.indexOf('"')) {
            return null;
        }

        str = str.replace('/', '\\').replace("\\\\", "\\");
        if (4 > str.length())
            return null;
        return str;
    }

    static public String PickupUrl(String str, boolean explicit) {

        if (-1 != str.indexOf('\t') || -1 != str.indexOf('\n')) {
            return null;
        }

        // 必需要有方案头
        int col_pos = str.indexOf("://");
        // -1==col_pos || 0==col_pos
        if (1 > col_pos || (col_pos == (str.length() - 3)))
            return null;
        if (7 > (str.length() - col_pos))
            return null;

        int col_pos2 = col_pos;
        str = str.toLowerCase();
        // 确定是什么方案
        // http://www.so.com/index.html
        for (int i = col_pos - 1; i >= 0; --i) {
            if ((str.charAt(i) >= 'a' && str.charAt(i) <= 'z')
                    || (str.charAt(i) >= '0' && str.charAt(i) <= '9')) {
                col_pos2--;
            } else {
                break;
            }
        }
        // 没有变化，或index错误
        // 考虑相隔过大的情况,hhhhhhhhhhhhhhttp://
        if (col_pos == col_pos2 || 0 > col_pos)
            return null;

        // 精确模式下，{方案后要有斜线，方案后要有点}
        str = str.substring(col_pos2);
        if (explicit) {
            if (str.indexOf('/', col_pos + 3) == -1)
                return null;
            if (str.indexOf('.', col_pos + 3) == -1)
                return null;
        }

        // 文件后缀暂时没法很好的判断

        str = str.replace('\\', '/')/*.replace("//", "/")*/;
        return str;
    }

    public static float SymbolRatio(String str) {
        // 符号占比高
        int sym_len = 0;
        for (int i = str.length() - 1; i >= 0; --i) {
            if (str.charAt(i) >= '0' && str.charAt(i) <= '9')
                continue;
            if (str.charAt(i) >= 'a' && str.charAt(i) <= 'z')
                continue;
            if (str.charAt(i) >= 'A' && str.charAt(i) <= 'Z')
                continue;
            sym_len++;
        }
        float sym_p = ((float) sym_len) / ((float) str.length());
        return sym_p;
    }

    /*
    *    "xxxxxx://active/?sid=%s";
    * */
    public static boolean IsUrl(String str) {
        Pattern pattern = Pattern.compile("^[\\w]+://[\\w\\-\\.]+");
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            return true;
        }
        return false;
    }

}
