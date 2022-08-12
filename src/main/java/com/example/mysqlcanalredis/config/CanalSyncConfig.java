package com.example.mysqlcanalredis.config;


import java.net.InetSocketAddress;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.client.*;
import com.example.mysqlcanalredis.utils.RedisUtil;
import org.apache.commons.lang.StringUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @ClassName CanalClient
 * @Description TODO
 * @Author 听秋
 * @Date 2022/4/21 18:58
 * @Version 1.0
 **/
@Configuration
public class CanalSyncConfig {

    private static String REDIS_DATABASE = "mall";
    private static String REDIS_KEY_ADMIN = "ums:admin";

    @Bean
    public static void canalSync() {
        // 创建链接,127.0.0.1是ip，11111是canal的端口号，默认是11111，这个在conf/canal.properties文件里配置，example是canal虚拟的模块名，
        // 在canal.properties文件canal.destinations= example 这段可以自行修改。canal是创建的数据库账号密码
//        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("192.168.1.2",
//                11111), "example", "canal", "canal");
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(AddressUtils.getHostIp(),
                11111), "example", "canal", "");
        int batchSize = 1000;
        int emptyCount = 0;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalEmtryCount = 1200;
//            while (emptyCount < totalEmtryCount) {
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    emptyCount++;
                    System.out.println("empty count : " + emptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    emptyCount = 0;
                    printEntry(message.getEntries());
                }

                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }

            //System.out.println("empty too many times, exit");
        }
//        catch (Exception e){
//            e.printStackTrace();
//        }
        finally {
            connector.disconnect();
        }
    }

    private static void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }
            String tableKey = null;
            if (entry.getHeader() != null) {
                tableKey = entry.getHeader().getSchemaName().concat(":").concat(entry.getHeader().getTableName());
            }
            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));
            System.out.println(rowChage.getRowDatasList().toString());

            for (RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                    redisDelete(tableKey,rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                    redisInsert(tableKey,rowData.getAfterColumnsList());
                } else if (eventType == EventType.UPDATE){
                    System.out.println("-------> before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------> after");
                    printColumn(rowData.getAfterColumnsList());
                    redisUpdate(tableKey,rowData.getAfterColumnsList());
                }else {
                    System.out.println("-------> before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------> after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void printColumn(List<Column> columns) {
        for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    private static void redisInsert(String prefixKey,List<Column> columns) {
        JSONObject json = new JSONObject();
        for (Column column : columns) {
            json.put(column.getName(), column.getValue());
        }
        if (columns.size() > 0) {
            if(!StringUtils.isBlank(prefixKey) ){
                RedisUtil.stringSet(prefixKey+":"+columns.get(1).getValue(), json.toJSONString());
            }else{
                RedisUtil.stringSet(REDIS_DATABASE + ":" + REDIS_KEY_ADMIN + ":"
                        + columns.get(1).getValue(), json.toJSONString());
            }

        }
    }

    private static void redisUpdate(String prefixKey,List<Column> columns) {
        JSONObject json = new JSONObject();
        for (Column column : columns) {
            json.put(column.getName(), column.getValue());
        }
        if (columns.size() > 0) {
            if (!StringUtils.isBlank(prefixKey)) {
                RedisUtil.stringSet(prefixKey + ":" + columns.get(1).getValue(), json.toJSONString());
            }else {
                RedisUtil.stringSet(REDIS_DATABASE + ":" + REDIS_KEY_ADMIN + ":"
                        + columns.get(1).getValue(), json.toJSONString());
            }

        }
    }

    private static void redisDelete(String prefixKey,List<Column> columns) {
        JSONObject json = new JSONObject();
        for (Column column : columns) {
            json.put(column.getName(), column.getValue());
        }
        if (columns.size() > 0) {
            if (!StringUtils.isBlank(prefixKey)) {
                RedisUtil.delKey(prefixKey + ":" + columns.get(1).getValue());
            }else{
                RedisUtil.delKey(REDIS_DATABASE + ":" + REDIS_KEY_ADMIN + ":" + columns.get(1).getValue());
            }

        }
    }

}
