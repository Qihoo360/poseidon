import InvertedIndex.plugin.Function.FilterFunction;

import InvertedIndex.plugin.Function.TokenFilter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

/**
 * Created by i-xuchongpeng on 2016/8/11.
 */
@RunWith(Parameterized.class)
public class FilterFunctionTest {

    @BeforeClass
    public static void setup() {
    }

    private FilterFunction Filter;
    private Set<String> input_set = new HashSet<>();
    private String input = "token_text1";
    private int result;

    public FilterFunctionTest(String filter_name, int precision, int result) {
        this.Filter = new FilterFunction(new TokenFilter(filter_name, precision));
        this.input_set.add("token_text1");
        this.input_set.add("token_text2");
        this.input_set.add("token_text3");
        this.result = result;
        //System.out.println(Filter.filter_.blacklist_);
    }

    @Parameterized.Parameters
    public static Collection data() {
        return Arrays.asList(new Object[][]{
                {"src/test/resources/filter1", 10, 1000},
                {"src/test/resources/filter1", 100, 100},
                {"src/test/resources/filter1", 1000, 10},
                {"src/test/resources/filter2", 10, 10000},
                {"src/test/resources/filter2", 100, 10000}
        });
    }

    @Test
    public void process() throws Exception {
        int res = 0;

        for (int i = 1; i < 10001; i++) {
            res += Filter.Process(input).size();
        }

        Assert.assertEquals(result, res);
    }

    @Test
    public void process1() throws Exception {
        int res = 0;

        for (int i = 1; i < 10001; i++) {
            res += Filter.Process(input_set).size();
        }
        Assert.assertEquals(result * 3, res);
    }

}