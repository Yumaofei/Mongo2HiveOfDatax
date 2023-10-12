package com.yumaofei;

import com.alibaba.fastjson2.JSONArray;
import com.yumaofei.util.ColumnUtil;
import com.yumaofei.util.MongoDBUtil;
import com.yumaofei.util.PropertiesUtil;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @program: Mongo2HiveOfDatax
 * @description:
 * @author: Mr.YMF
 * @create: 2023-10-10 16:14
 **/

public class CreateSql {
    //根据列名和提供的表名，生成创建表的sql
    public String GetCreateTable(LinkedHashMap<String, String> columnsMap,String collection,String fromSystem,String tableType,Boolean isPartition,String tableComment){
        //处理所获字段集中的类型
        String body = CreateTableColumnFormat(columnsMap);

        //将column中的大写字符转化为下划线加对应小写字符
        body = ColumnUtil.Up2Low(body);

        //表名处理
        String tableName = ColumnUtil.Up2Low(collection);
        if (fromSystem.equals("erp"))
            tableName = "ods_" + tableName + tableType;
        else
            tableName = "ods_"+ fromSystem + "_" + tableName + tableType;
        String partitionCreateSql = null;
        if (isPartition.equals(true))
            partitionCreateSql = "partitioned by (dt string comment'分区时间')\n";
        //将读取的columns字符串转化为hive建表语句
        String head = "create external table ods." + tableName + "(\n\t";
        String tail = "\n)comment '" + tableComment + "'\n" +
                partitionCreateSql +
                "row format delimited fields terminated by '\\t'\n" +
                "NULL defined as ''\n" +
                "stored as orc\n" +
                "location '/data/hadoop/hive/warehouse/ods.db/" + fromSystem + "/" + tableName + "/'\n" +
                "tblproperties ('orc.compress' = 'snappy')";
        return head+body+tail;
    }

    public String CreateTableColumnFormat(LinkedHashMap<String, String> columnsMap){
        String columns = "";
        for (String key : columnsMap.keySet()){
            String value = columnsMap.get(key);
            if (value.equals("double"))
                columns += key + " decimal(16,4) comment'',\n\t";
            else
                columns += key + " string comment'',\n\t";
        }
        columns = columns.trim();
        columns = columns.substring(0, columns.length() - 1);
        return columns;
    }

    /**
     * 读取配置文件
     * @param url
     * @return
     */
    public Properties ReadProperties(String url){
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