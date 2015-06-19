package data.sync.common;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hesiyuan on 15/1/29.
 */
public class HttpUtil {

    private static PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    static {
        cm.setMaxTotal(200);
        cm.setDefaultMaxPerRoute(200);
    }
    private static CloseableHttpClient httpClient = HttpClients.custom()
            .setConnectionManager(cm)
            .build();
    private static final Log LOG =
            LogFactory.getLog(HttpUtil.class);
    public static enum HttpType {
        POST, GET, PUT, DELETE
    }
    // url参数填充
    private static String fillUrl(String url, Map<String, String> map)
            throws Exception {
        StringBuilder sb = new StringBuilder(url);
        if (map.size()>0) {
            sb.append('?');
            for(String key:map.keySet()){
                sb.append(key);
                sb.append('=');
                sb.append(URLEncoder.encode(map.get(key),"UTF-8"));
                sb.append('&');
            }
            return sb.substring(0,sb.length()-1);
        }else{
            return url;
        }

    }
    public static HttpUriRequest getRequest(String url,
                                            Map<String, String> map, HttpType type,int conTimeout,int soTimeout) throws Exception {
        RequestConfig config =RequestConfig.custom()
                .setSocketTimeout(soTimeout)
                .setConnectTimeout(conTimeout)
                .setConnectionRequestTimeout(soTimeout)
                .build();
        switch(type){
            case GET:{
                HttpGet get  = new HttpGet(fillUrl(url,map));
                get.setConfig(config);
                return get;
            }
            case POST:{
                HttpPost post = new HttpPost(url);
                List<NameValuePair> params = new ArrayList<NameValuePair>();
                for(String key:map.keySet()){
                    NameValuePair name;
                    name = new BasicNameValuePair(key,map.get(key));
                    params.add(name);
                }
                UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params, Consts.UTF_8);
                post.setEntity(entity);
                post.setConfig(config);
                return post;
            }
            default:throw new RuntimeException(type+" Not Supported!! ");
            //Todo PUT,DELETE
        }

    }
    public static CloseableHttpResponse execute(String url,
                                         Map<String, String> map, HttpType type,int times,int conTimeout,int soTimeout) throws Exception{

        CloseableHttpResponse response = httpClient.execute(getRequest(url, map, type,conTimeout,soTimeout));
        return response;
    }
}
