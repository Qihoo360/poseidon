package InvertedIndex.plugin.Function;

import java.io.BufferedReader;
import java.io.FileReader;
import java.security.PublicKey;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */ //过滤器对象，内置黑名单和过滤概率发生器
//0 表示全部过滤
//非0值precision_,表示按1/precision_过滤输出
public class TokenFilter {
    public Set<String> blacklist_ = new HashSet<>();
    private int precision_;
    private int worker_count_ = 0;

    public boolean Worker() {

        worker_count_ += 1;
        worker_count_ %= precision_;
        //System.err.println(worker_count_);
        return (worker_count_ == 5);
    }

    public TokenFilter(String file_name, int precision) {
        precision_ = precision;
        loadFilterTokenFile(file_name);
    }

    public void loadFilterTokenFile(String file_name) {
        try {
            //System.out.println(TokenFilter.class.getResource("/").getFile());
            BufferedReader fin = new BufferedReader(new FileReader(file_name));
            String ss = fin.readLine();
            while (ss != null) {
                blacklist_.add(ss);
                ss = fin.readLine();
            }
        } catch (Exception e) {
            System.err.println("load " + file_name + " failed :" + e.toString());
            e.printStackTrace();
            System.exit(-1);
        }
    }

    //for test
    public void get() {

    }

    public boolean PassBy(String token) {
        if (!blacklist_.contains(token)) {
            //System.err.println(token + " not in black~~~~~~`");
            return true;
        }
        if (precision_ == 0) return false;
        return Worker();
    }
}
