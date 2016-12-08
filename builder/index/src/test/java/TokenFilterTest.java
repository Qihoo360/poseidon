import InvertedIndex.plugin.Function.TokenFilter;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
public class TokenFilterTest {
    @Test
    public void worker() throws Exception {

    }

    @Test
    public void loadFilterTokenFile() throws Exception {
        TokenFilter test1 = new TokenFilter("src/test/resources/filter1", 10);
        /*
        System.out.println(test1.blacklist_);
        if (test1.PassBy("token_test")) System.out.println("1");
        System.out.println(test1.getworkcount());
        if (test1.PassBy("token_test1")) System.out.println("2");
        System.out.println(test1.getworkcount());
        if (test1.PassBy("token_test1")) System.out.println("3");
        System.out.println(test1.getworkcount());
        if (test1.PassBy("token_test1")) System.out.println("4");
        System.out.println(test1.getworkcount());
        if (test1.PassBy("token_test1")) System.out.println("5");
        System.out.println(test1.getworkcount());
                //Assert.assertEquals(test1.blacklist_.size(),3);
                */
    }

    @Test
    public void passby() throws Exception {
        System.err.println(Test.class.getResource("/").getFile());
    }

}