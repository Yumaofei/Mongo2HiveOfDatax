package com.yumaofei;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.yumaofei.util.ColumnUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @program: Mongo2HiveOfDatax
 * @description:
 * @author: Mr.YMF
 * @create: 2023-10-10 15:29
 **/

public class CreateJson {
    public JSONObject ddx4mg(LinkedHashMap<String, String> columnsMap,String collection,String fromSystem,String tableType,Boolean isPartition){
        //创建datax
        JSONObject datax = new JSONObject();
        //创建datax的core和job
        JSONObject core = new JSONObject();
        JSONObject job = new JSONObject();
        //设置datax
        datax.put("core",core);
        datax.put("job",job);

//      -------------------core的相关参数设置-------------------
        //创建datax的core的transport
        JSONObject coreTransport = new JSONObject();
        //设置datax的core
        core.put("transport",coreTransport);
        //创建datax的core的transport的channel
        JSONObject coreTransportChannel = new JSONObject();
        //设置datax的core的transport
        coreTransport.put("channel",coreTransportChannel);
        //创建datax的core的transport的channel的speed
        JSONObject coreTransportChannelSpeed = new JSONObject();
        //设置datax的core的transport的channel
        coreTransportChannel.put("speed",coreTransportChannelSpeed);
        //设置datax的core的transport的channel的speed
        coreTransportChannelSpeed.put("byte",104857600);

//      -------------------job的相关参数设置-------------------
        //创建datax的job的setting和content
        JSONObject jobSetting = new JSONObject();
        JSONArray jobContent = new JSONArray();
        //设置datax的job
        job.put("setting",jobSetting);
        job.put("content",jobContent);
        //创建datax的job的setting的speed
        JSONObject jobSettingSpeed = new JSONObject();
        //设置datax的job的setting
        jobSetting.put("speed",jobSettingSpeed);
        //设置datax的job的setting的speed
        jobSettingSpeed.put("channel",1);
        //创建datax的job的content的数组中对象（reader和writer）
        JSONObject jobContentarr1 = new JSONObject();
        //设置datax的job的content
        jobContent.set(0,jobContentarr1);
        //创建datax的job的content的数组对象的reader和writer
        JSONObject jobContentReader = new JSONObject();
        JSONObject jobContentWriter = new JSONObject();
        //设置datax的job的content数组中对象（reader和writer）
        jobContentarr1.put("reader",jobContentReader);
        jobContentarr1.put("writer",jobContentWriter);

//      -------------------reader的相关参数设置-------------------
        //创建datax的job的content的reader的parameter
        JSONObject jobReaderParameter = new JSONObject();
        //设置datax的job的content的reader
        jobContentReader.put("name","mongodbreader");
        jobContentReader.put("parameter",jobReaderParameter);
        //设置datax的job的content的reader的parameter的address
        ArrayList<String> address = new ArrayList<>();
        address.add("${zhiziSorceDbAddress}");
        jobReaderParameter.put("address",address);
        //设置datax的job的content的reader的parameter的userName
        jobReaderParameter.put("userName","${zhiziSorceDbUsername}");
        //设置datax的job的content的reader的parameter的userPassword
        jobReaderParameter.put("userPassword","${zhiziSorceDbPassword}");
        //设置datax的job的content的reader的parameter的dbName
        jobReaderParameter.put("dbName","${zhiziSorceDbDatabase}");
        //设置datax的job的content的reader的parameter的collectionName
        jobReaderParameter.put("collectionName",collection);
        //设置datax的job的content的reader的parameter的column，先处理传递的columns
        JSONArray readerParameterColumn = readerParameterColumnFCT(columnsMap);
        jobReaderParameter.put("column",readerParameterColumn);

//      -------------------writer的相关参数设置-------------------
        //创建datax的job的content的writer的parameter
        JSONObject jobWriterParameter = new JSONObject();
        //设置datax的job的content的writer
        jobContentWriter.put("name","hdfswriter");
        jobContentWriter.put("parameter",jobWriterParameter);
        //设置datax的job的content的writer的parameter的defaultFS
        jobWriterParameter.put("defaultFS","${defaultFS}");
        //设置datax的job的content的writer的parameter的path
        //path包括地址、目录，其中目录包含库、系统、表名、是否分区
        String path = "${hiveErpDbHdfsDir}";
        path += "/ods.db/";
        path += fromSystem + "/";
        String tableName = ColumnUtil.Up2Low(collection);
        if (fromSystem.equals("erp"))
            tableName = "ods_" + tableName + tableType;
        else
            tableName = "ods_"+ fromSystem + "_" + tableName + tableType;
        path += tableName + "/";
        if (isPartition.equals(true))
            path += "dt=${dt}/";
        jobWriterParameter.put("path",path);
        //设置datax的job的content的writer的parameter的fileType
        jobWriterParameter.put("fileType","orc");
        //设置datax的job的content的writer的parameter的compress
        jobWriterParameter.put("compress","snappy");
        //设置datax的job的content的writer的parameter的fileName

        jobWriterParameter.put("fileName",tableName);
        //设置datax的job的content的writer的parameter的writeMode
        jobWriterParameter.put("writeMode","truncate");
        //设置datax的job的content的writer的parameter的fieldDelimiter
        jobWriterParameter.put("fieldDelimiter","\t");
        //设置datax的job的content的writer的parameter的column，，先处理传递的columns
        JSONArray writerParameterColumn = writerParameterColumnFCT(columnsMap);
        jobWriterParameter.put("column",writerParameterColumn);

        return datax;
    }

    private JSONArray readerParameterColumnFCT(LinkedHashMap<String, String> columnsMap){
        ArrayList<LinkedHashMap<String, String>> columnsList = new ArrayList<>();
        for (String key : columnsMap.keySet()){
            LinkedHashMap<String, String> map = new LinkedHashMap<>();
            map.put("name",key);
            map.put("type",columnsMap.get(key));
            columnsList.add(map);
        }
        //创建datax的job的content的reader的parameter的column
        return new JSONArray(columnsList);
    }

    private JSONArray writerParameterColumnFCT(LinkedHashMap<String, String> columnsMap){
        ArrayList<LinkedHashMap<String, String>> columnsList = new ArrayList<>();
        for (String key : columnsMap.keySet()){
            LinkedHashMap<String, String> map = new LinkedHashMap<>();
            String up2LowKey = ColumnUtil.Up2Low(key);
            map.put("name",up2LowKey);
            map.put("type","string");
            columnsList.add(map);
        }
        //创建datax的job的content的writer的parameter的column
        return new JSONArray(columnsList);
    }
}
