package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.common.namesrv.NameServerUpdateCallback;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.utils.HttpTinyClient;

import java.io.IOException;


/**
 * @author shizhen(十真) 2023/08/03
 * NameSrv自定义寻址
 */
public class SzzTopAddressing implements TopAddressing {


    @Override
    public String fetchNSAddr() {
        // 根据环境标返回
        String env = "";

        if(env.equals("test")){
            return "127.0.0.1:9876;127.0.0.1:9877";
        }else if (env.equals("test")){
            return "127.0.0.1:9876;127.0.0.1:9879";
        }

        // 又或者 也去访问对应的链接
        try {
            HttpTinyClient.HttpResult result = null;
            String url  = "http://localhost:7003/rocketmq/getnsaddr?env="+env;
            result = HttpTinyClient.httpGet(url, null, null, "UTF-8", 3000);
            if (200 == result.code) {
                String responseStr = result.content;
                if (responseStr != null) {
                    return clearNewLine(responseStr);
                } else {

                }
            } else {

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;

    }

    @Override
    public void registerChangeCallBack(NameServerUpdateCallback changeCallBack) {

    }


    private static String clearNewLine(final String str) {
        String newString = str.trim();
        int index = newString.indexOf("\r");
        if (index != -1) {
            return newString.substring(0, index);
        }

        index = newString.indexOf("\n");
        if (index != -1) {
            return newString.substring(0, index);
        }

        return newString;
    }
}
