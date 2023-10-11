package com.yumaofei;

import com.alibaba.fastjson2.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.yumaofei.util.MongoDBUtil;
import com.yumaofei.util.PropertiesUtil;
import org.bson.Document;

import java.util.*;

/**
 * @program: Mongo2HiveOfDatax
 * @description:
 * @author: Mr.YMF
 * @create: 2023-10-09 18:04
 **/

public class MG2HV {
    public static void main(String[] args) {
        //接收传递的参数
        String mongodbLocalhost = args[0];
        int mongodbPort = Integer.parseInt(args[1]);
        String mongodbUsrename = args[2];
        String mongodbPassword = args[3];
        String mongodbDatabase = args[4];
        String mongodbCollection = args[5];
        String fromSystem = args[6];
        String tableType = args[7];
        Boolean isPartition = Boolean.valueOf(args[8]);
        String tableComment = args[9];

        MongoDBUtil mongoDBUtil = new MongoDBUtil(mongodbLocalhost,mongodbPort,mongodbUsrename,mongodbPassword,mongodbDatabase);
        mongoDBUtil.certifyMongoClient();

        MongoDatabase db = mongoDBUtil.getDatabase(mongodbDatabase);

        //通过库获取集合
//        Properties pro = PropertiesUtil.ReadProperties();
        MongoCollection<Document> collection = db.getCollection(mongodbCollection);

        // 查询所有的数据
        int pageIndex = 1;
        int pageSize = 1000;
        BasicDBObject ship_confirm_date = new BasicDBObject();
        LinkedHashMap<String, String> data = new LinkedHashMap<String, String>();
        while (pageIndex < 10) {
            FindIterable findIterable = collection.find(ship_confirm_date).skip((pageIndex - 1) * pageSize * 10).sort(new BasicDBObject("_id",-1)).limit(pageSize);
            MongoCursor<Document> cur = findIterable.iterator();
            while (cur.hasNext()) {
                Map<String, Object> object = cur.next();
                for (String key : object.keySet()) {
                    Object obj = object.get(key);
                    try {
                        if (obj instanceof Document) {
                            data.put(key, "json");
                        } else if (obj instanceof List) {
                            data.put(key, "array");
                        } else if (obj instanceof Double) {
                            data.put(key,"double");
                        } else { //其他一律按string处理
                            data.put(key, "string");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println(obj);
                    }
                }
            }
            pageIndex++;
        }

        mongoDBUtil.closeDB();

/*        Set<String> keySet = data.keySet();
        Iterator<String> iterator = keySet.iterator();
        while (iterator.hasNext()){
            String next = iterator.next();
            Object o = data.get(next);
            System.out.println("key:"+next+"\tvalue:"+o.toString());
        }*/

        CreateJson createJson = new CreateJson();
        JSONObject jsonObject = createJson.ddx4mg(data,mongodbCollection,fromSystem,tableType,isPartition);
        System.out.println(jsonObject);

        CreateSql createSql = new CreateSql();
        String s = createSql.GetCreateTable(data,mongodbCollection,fromSystem,tableType,isPartition,tableComment);
        System.out.println(s);

    }
}
