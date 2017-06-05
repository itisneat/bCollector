package org.bitcoin.common;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Map;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HttpUtils.class);

    public static String USER_AGENT = "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:26.0) Gecko/20100101 Firefox/26.0";

    public static Connection getConnectionForPost(String url, Map<String, String> datas) {
        url = appendHttpString(url);
        Connection connection = Jsoup.connect(url)
                .userAgent(USER_AGENT).timeout(5000)
                .method(Connection.Method.POST);
        if (datas != null && !datas.isEmpty()) {
            connection.data(datas);
        }
        return connection;
    }

    private static String appendHttpString(String url) {
        if (!url.startsWith("http")) {
            url = "http://" + url;
        }
        return url;
    }

    public static Connection getConnectionForGetNoCookies(String url, Map<String, String>... datas) {
        url = appendHttpString(url);
        Connection connection = Jsoup.connect(url).userAgent(USER_AGENT).ignoreContentType(true).timeout(5000);
        if (datas != null && datas.length > 0 && !datas[0].isEmpty()) {
            connection.data(datas[0]);
        }

        return connection;
    }

    public static Connection getConnectionForGet(String url, Map<String, String>... datas) {
        return getConnectionForGetNoCookies(url, datas);
    }

    public static String getContentForGet(String url, int timeout) {
        try {
            Document objectDoc;
            try {
                Connection connection = getConnectionForGetNoCookies(url).timeout(timeout);
                objectDoc = connection.get();
            } catch (SocketTimeoutException e) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e1) {
                    // ignore
                }
                Connection connection = getConnectionForGetNoCookies(url).timeout(timeout);
                objectDoc = connection.get();
            }
            return objectDoc.body().text();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void appadd(String jsonString) {

        try {
            //创建连接
            URL url = new URL("http://120.55.171.131:42421/api/put");
            HttpURLConnection connection = (HttpURLConnection) url
                    .openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setUseCaches(false);
            
//            connection.setInstanceFollowRedirects(true);
            connection.setRequestProperty("Content-Type",
                    "application/x-www-form-urlencoded");
            connection.setChunkedStreamingMode(4096);

            connection.connect();

            //POST请求
            DataOutputStream out = new DataOutputStream(
                    connection.getOutputStream());

            out.writeBytes(jsonString);
            out.flush();
            out.close();

            //读取响应
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String lines;
            StringBuffer sb = new StringBuffer("");
            while ((lines = reader.readLine()) != null) {
                lines = new String(lines.getBytes(), "utf-8");
                sb.append(lines);
            }
            System.out.println(sb);
            reader.close();
            // 断开连接
            connection.disconnect();
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
