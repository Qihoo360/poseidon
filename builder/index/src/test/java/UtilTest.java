import InvertedIndex.plugin.Util;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.Assert;

/**
 * Util Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>Aug 2, 2016</pre>
 */
public class UtilTest {

    @Before
    public void before() throws Exception {
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: IsUnReadable(String str)
     */
    @Test
    public void testIsUnReadable() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: IsDigit(String str)
     */
    @Test
    public void testIsDigit() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: IsHexadecimal(String str)
     */
    @Test
    public void testIsHexadecimal() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: IsChinese(String str)
     */
    @Test
    public void testIsChinese() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: Split(String line, String sptr)
     */
    @Test
    public void testSplit() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: ParseIp(String val)
     */
    @Test
    public void testParseIp() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: ParseIpSet(String val)
     */
    @Test
    public void testParseIpSet() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: ParsePath(String val, Set<String> set, boolean path_combo)
     */
    @Test
    public void testParsePathForValSetPath_combo() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: ParsePath(String val, Set<String> set)
     */
    @Test
    public void testParsePathForValSet() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: ParsePath(String val)
     */
    @Test
    public void testParsePathVal() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: ParseUrl(String val, Set<String> set)
     */
    @Test
    public void testParseUrlForValSet() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: ParseUrl(String val)
     */
    @Test
    public void testParseUrlVal() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: Base64DecoderStr(String str, boolean flag)
     */
    @Test
    public void testBase64DecoderStrForStrFlag() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: Base64EncoderStr(String str, boolean flag)
     */
    @Test
    public void testBase64EncoderStr() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: Base64DecoderStr(String str)
     */
    @Test
    public void testBase64DecoderStrStr() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: ReadFile(String Path)
     */
    @Test
    public void testReadFile() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: Compress(String data)
     */
    @Test
    public void testCompressData() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: Compress(InputStream is, OutputStream os)
     */
    @Test
    public void testCompressForIsOs() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: Gzuncompress(byte []data)
     */
    @Test
    public void testGzuncompress() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: TrimStr(String stream, String trimstr)
     */
    @Test
    public void testTrimStr() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: PickupPath(String str, boolean explicit)
     */
    @Test
    public void testPickupPath() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: PickupUrl(String str, boolean explicit)
     */
    @Test
    public void testPickupUrl() throws Exception {
//TODO: Test goes here... 
    }

    /**
     * Method: SymbolRatio(String str)
     */
    @Test
    public void testSymbolRatio() throws Exception {
//TODO: Test goes here...
    }

    /**
     * Method: IsUrl(String str)
     */
    @Test
    public void testIsUrl() throws Exception {
//TODO: Test goes here...
        String str = "baidu://active/?sid=%s";
        boolean ok = Util.IsUrl(str);
        Assert.assertEquals(ok, true);

        str = "//active/?sid=%s";
        ok = Util.IsUrl(str);
        Assert.assertEquals(ok, false);

        str = "https://active/?sid=%s";
        ok = Util.IsUrl(str);
        Assert.assertEquals(ok, true);
    }

} 
