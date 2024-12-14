package com.atguigu.edu.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.bean.TableProcess;
import com.atguigu.edu.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * Title: TableProcessFunction
 * Create on: 2024/12/14 14:46
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  对关联后的数据进行处理(过滤出维度数据)
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>> {

    private Map<String,TableProcess> configMap = new HashMap<>();
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        String sql = "select * from edu_config.table_process";
        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcess> tableProcessList = JdbcUtil.queryList(mysqlConnection, sql, TableProcess.class, true);
        for (TableProcess tableProcess : tableProcessList) {
            configMap.put(tableProcess.getSourceTable(),tableProcess);
        }
        //System.out.println("广播状态中的配置信息为："+configMap);
        JdbcUtil.closeMysqlConnection(mysqlConnection);
    }

    @Override
    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {
        //{"database":"gmall0620","data":{"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"aaa","id":1},"type":"update","table":"base_trademark","ts":1732932983}
        //System.out.println(jsonObj);
        //获取当前操作的业务数据库表表名
        String table = jsonObj.getString("table");
        // 数据进来了
        //System.out.println("jsonObj:"+jsonObj);
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        //根据表名到广播状态中获取对应的配置信息
        TableProcess tableProcess = null;

        if((tableProcess = broadcastState.get(table)) != null
                ||(tableProcess = configMap.get(table)) != null){
            //说明当前处理的这条数据 是维度数据  需要将这条维度数据data部分内容以及对应的配置对象封装为二元组向下游传递
            //{"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"aaabbbccc","id":1}
            //System.out.println("当前处理的维度数据为："+broadcastState);
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            //在向下游传递数据前  过滤掉不需要传递的字段
            String sinkColumns = tableProcess.getSinkColumns();
            deleteNotNeedColumns(dataJsonObj,sinkColumns);
            //在向下游传递数据前  需要补充操作类型字段
            //{"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"aaabbbccc","id":1,"type":"update"}
            String type = jsonObj.getString("type");
            dataJsonObj.put("type",type);
            collector.collect(Tuple2.of(dataJsonObj,tableProcess));
        }
    }

    @Override
    public void processBroadcastElement(TableProcess tp, BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>.Context context, Collector<Tuple2<JSONObject, TableProcess>> collector) throws Exception {
        //获取对配置表进行操作的类型
        String op = tp.getOp();
        //获取广播状态
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);

        String sourceTable = tp.getSourceTable();
        if("d".equals(op)){
            //说明从配置表中删除了一条配置信息  从广播状态中将对应的配置删除
            broadcastState.remove(sourceTable);
            configMap.remove(sourceTable);
        }else {
            //说明对配置表进行了读取、添加或者修改操作   将最新的配置信息放到广播状态中
            broadcastState.put(sourceTable,tp);
            configMap.put(sourceTable,tp);
        }
    }
    //过滤掉不需要保留的字段
    // 传入值
    //dataJsonObj: {"tm_name":"Redmi","create_time":"2021-12-14 00:00:00","logo_url":"aaabbbccc","id":1}
    //sinkColumns: id,tm_name
    private static void deleteNotNeedColumns(JSONObject dataJsonObj,String sinkColumns){
        List<String> columnsList = Arrays.asList(sinkColumns.split(","));
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(entry -> !columnsList.contains(entry.getKey()));
    }
}
