package InvertedIndex;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

/**
 * Created by liwei on 9/23/16.
 */
public class MetaSetter {

    private String url_;

    public MetaSetter(String url) {
        url_ = url;
    }

/*
    public bool MetaPost(Map<String, String> meta) {
        for(Map.Entry<String,String> entry : meta.entrySet()) {
        }
    }
*/


    public String Post(String body) {
        HttpClient httpclient = new DefaultHttpClient();
        String retbody = null;
        try {
            HttpPost httpPost = new HttpPost(url_);
            //System.err.println("executing request " + httpPost.getURI());
            httpPost.setEntity(new StringEntity(body));

            HttpResponse response = httpclient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                //System.err.println("Response content length: " + entity.getContentLength());
                retbody = EntityUtils.toString(entity);
                //System.err.println(retbody);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            httpclient.getConnectionManager().shutdown();
        }
        return retbody;
    }
}
