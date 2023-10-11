package com.yumaofei.util;

/**
 * @program: Mongo2HiveOfDatax
 * @description:
 * @author: Mr.YMF
 * @create: 2023-10-10 18:22
 **/

public class ColumnUtil {
    public static String Up2Low(String str) {
        //如果列以下划线开头，去除下划线
        if(str.startsWith("_")) {
            str = str.replaceFirst("_", "");
        }
        str = str.replaceAll(",_", ",");

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (Character.isUpperCase(ch)) {
                result.append('_');
            }
            result.append(ch);
        }
        String lowerCase = result.toString().toLowerCase();
        return lowerCase;
    }
}
