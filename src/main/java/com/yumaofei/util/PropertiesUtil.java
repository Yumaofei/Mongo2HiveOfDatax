package com.yumaofei.util;

import java.io.*;
import java.util.Properties;

/**
 * @program: Mongo2HiveOfDatax
 * @description:
 * @author: Mr.YMF
 * @create: 2023-10-10 16:59
 **/

public class PropertiesUtil {

    private static String url = "mg2hv.properties";

    public static Properties ReadProperties(){
        Properties pro = new Properties();
        try {
            InputStream resourceAsStream = MongoDBUtil.class.getClassLoader().getResourceAsStream(url);
            BufferedReader bf = new BufferedReader(new InputStreamReader(resourceAsStream,"utf-8"));
            pro.load(bf);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return pro;
    }
}
