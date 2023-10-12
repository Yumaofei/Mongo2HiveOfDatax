package com.yumaofei.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @program: Mongo2HiveOfDatax
 * @description:
 * @author: Mr.YMF
 * @create: 2023-10-10 14:09
 **/

public class MongoDBUtil {

    String mongodbLocalhost;
    int mongodbPort;
    String mongodbUsrename;
    String mongodbPassword;
    String mongodbDatabase;

    /**
     * MongoDB连接对象
     */
    private MongoClient client;

    /**
     * MongoDBUtil对象
     */
    private static MongoDBUtil instance;

    public MongoDBUtil() {
    }

    public MongoDBUtil(String mongodbLocalhost,String mongodbUsrename,String mongodbPassword,String mongodbDatabase) {
        this.mongodbLocalhost = mongodbLocalhost;
        this.mongodbUsrename = mongodbUsrename;
        this.mongodbPassword = mongodbPassword;
        this.mongodbDatabase = mongodbDatabase;
    }

    public MongoDBUtil(String mongodbLocalhost,int mongodbPort,String mongodbUsrename,String mongodbPassword,String mongodbDatabase) {
        this.mongodbLocalhost = mongodbLocalhost;
        this.mongodbPort = mongodbPort;
        this.mongodbUsrename = mongodbUsrename;
        this.mongodbPassword = mongodbPassword;
        this.mongodbDatabase = mongodbDatabase;
    }

    /**
     * 获取MongoDBUtil对象
     * @return  MongoDBUtil
     */
    public static MongoDBUtil getInstance(){
        if(instance == null){
            instance = new MongoDBUtil();
        }
        return instance;
    }

    /**
     * 关闭连接对象
     */
    public void closeDB(){
        if(client != null){
            client.close();
        }
        client = null;
    }

    /**
     * 不需要认证获取连接对象
     */
    public void mongoClient(String host,int post){
        try {
            //获取mongodb连接对象
            client = new MongoClient("localhost",27017);
        } catch (Exception e) {
//            logger.info("不需要认证获取连接对象失败,{}",e);
            e.printStackTrace();
        }
    }

    /**
     * 需要认证获取连接对象
     */
    public void certifyMongoClient(){
//        Properties properties = PropertiesUtil.ReadProperties();
//        String mongodbLocalhost = properties.getProperty("mongodb_localhost");
//        int mongodbPort = Integer.parseInt(properties.getProperty("mongodb_port"));
//        String mongodbUsrename = properties.getProperty("mongodb_usrename");
//        String mongodbPassword = properties.getProperty("mongodb_password");
//        String mongodbDatabase = properties.getProperty("mongodb_database");
        try {
            //连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址
            //ServerAddress()两个参数分别为 服务器地址 和 端口
//            ServerAddress serverAddress = new ServerAddress(mongodbLocalhost,mongodbPort);
            ServerAddress serverAddress = new ServerAddress(mongodbLocalhost);
            List<ServerAddress> addrs = new ArrayList<ServerAddress>();
            addrs.add(serverAddress);

            //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
            MongoCredential credential = MongoCredential.createScramSha1Credential(mongodbUsrename, mongodbDatabase, mongodbPassword.toCharArray());
            List<MongoCredential> credentials = new ArrayList<MongoCredential>();
            credentials.add(credential);

            //通过连接认证获取MongoDB连接
            client = new MongoClient(addrs,credentials);
        } catch (Exception e) {
//            logger.info("需要认证的获取连接对象失败,{}",e);
            e.printStackTrace();
        }
    }

    /**
     * 获取数据库对象
     * @param databaseName  数据库名
     * @return  MongoDatabase
     */
    public MongoDatabase getDatabase(String databaseName){
        if(client == null){
            certifyMongoClient();
        }
        MongoDatabase database = client.getDatabase(databaseName);
        return database;
    }
}
