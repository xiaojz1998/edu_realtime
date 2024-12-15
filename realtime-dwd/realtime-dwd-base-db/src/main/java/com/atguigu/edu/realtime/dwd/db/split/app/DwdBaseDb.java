package com.atguigu.edu.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwdTableProcess;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import com.atguigu.edu.realtime.common.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * Title: DwdBaseDb
 * Create on: 2024/12/15 22:38
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *       zk、kafka、maxwell、DwdBaseDb
 */
public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(
                10019,
                4,
                "dwd_base_db",
                Constant.TOPIC_DB
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 对流中数据进行转换并ETL jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(s);
                            String type = jsonObject.getString("type");
                            if (!type.startsWith("bootstrap-")) {
                                collector.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            System.out.println("不是标准的json");
                        }
                    }
                }
        );
        //jsonObjDS.print("jsonObjDS");
        // TODO 使用flinkCDC 读取配置表信息
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("dwd_table_process");
        DataStreamSource<String> mySqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        SingleOutputStreamOperator<DwdTableProcess> tpDS = mySqlStrDS.map(
                new MapFunction<String, DwdTableProcess>() {
                    @Override
                    public DwdTableProcess map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        String op = jsonObject.getString("op");
                        DwdTableProcess dwdTableProcess = null;
                        if ("d".equals(op)) {
                            //说明从配置表删除了一条配置信息，从before属性中获取删除前的配置
                            dwdTableProcess = jsonObject.getObject("before", DwdTableProcess.class);
                        } else {
                            //说明对配置表进行了读取、添加、修改操作，从after属性中获取最新的配置信息
                            dwdTableProcess = jsonObject.getObject("after", DwdTableProcess.class);
                        }
                        //补充操作类型到对象上
                        dwdTableProcess.setOp(op);
                        return dwdTableProcess;
                    }
                }
        ).setParallelism(1);
        tpDS.print("tpDS");

        // TODO 广播流配置
        MapStateDescriptor<String, DwdTableProcess> mapStateDescriptor = new MapStateDescriptor<>("mapStateDescriptor", String.class, DwdTableProcess.class);
        BroadcastStream<DwdTableProcess> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //TODO 将主流业务数据和广播流配置信息进行关联---connect
        BroadcastConnectedStream<JSONObject, DwdTableProcess> connectDS = jsonObjDS.connect(broadcastDS);
        //TODO 对关联后的数据进行处理---process
        SingleOutputStreamOperator<Tuple2<JSONObject, DwdTableProcess>> realDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, DwdTableProcess, Tuple2<JSONObject, DwdTableProcess>>() {

                    private Map<String, DwdTableProcess> configMap = new HashMap<>();

                    //将配置表的配置信息预加载到configMap
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                        String sql = "select * from edu_config.dwd_table_process";
                        List<DwdTableProcess> tableProcessDwdList = JdbcUtil.queryList(mysqlConnection, sql, DwdTableProcess.class, true);
                        for (DwdTableProcess dwdTableProcess : tableProcessDwdList) {
                            String sourceTable = dwdTableProcess.getSourceTable();
                            String sourceType = dwdTableProcess.getSourceType();
                            String key = getKey(sourceTable, sourceType);
                            configMap.put(key, dwdTableProcess);
                        }
                        JdbcUtil.closeMysqlConnection(mysqlConnection);
                    }

                    //对主流数据进行处理
                    @Override
                    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, DwdTableProcess, Tuple2<JSONObject, DwdTableProcess>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, DwdTableProcess>> collector) throws Exception {
                        //获取表名
                        String table = jsonObj.getString("table");
                        //获取对业务数据库表进行操作的类型
                        String type = jsonObj.getString("type");
                        //拼接key
                        String key = getKey(table, type);
                        //获取广播状态
                        ReadOnlyBroadcastState<String, DwdTableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                        //根据key到广播状态以及configMap中获取对应的配置
                        DwdTableProcess dwdTableProcess = null;
                        if ((dwdTableProcess = broadcastState.get(key)) != null
                                || (dwdTableProcess = configMap.get(key)) != null) {
                            //如果配置对象不为空，将其中data部分内容以及对应的配置对象 封装为二元组向下游传递
                            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                            //过滤掉不需要传递的字段
                            String sinkColumns = dwdTableProcess.getSinkColumns();
                            deleteNotNeedColumns(dataJsonObj, sinkColumns);
                            //在向下游传递数据前，补充ts字段
                            dataJsonObj.put("ts", jsonObj.getLong("ts"));
                            collector.collect(Tuple2.of(dataJsonObj, dwdTableProcess));
                        }
                    }

                    @Override
                    public void processBroadcastElement(DwdTableProcess tp, BroadcastProcessFunction<JSONObject, DwdTableProcess, Tuple2<JSONObject, DwdTableProcess>>.Context context, Collector<Tuple2<JSONObject, DwdTableProcess>> collector) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tp.getOp();
                        //获取广播状态
                        BroadcastState<String, DwdTableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
                        String sourceTable = tp.getSourceTable();
                        String sourceType = tp.getSourceType();
                        String key = getKey(sourceTable, sourceType);
                        if ("d".equals(op)) {
                            //说明从配置表中删除了一条配置信息，需要从广播状态以及configMap中删除对应的配置
                            broadcastState.remove(key);
                            configMap.remove(key);
                        } else {
                            //说明对配置表进行了读取、添加、更新操作，需要将最新的配置put到广播状态以及configMap中
                            broadcastState.put(key, tp);
                            configMap.put(key, tp);
                        }
                    }

                    public String getKey(String sourceTable, String sourceType) {
                        String key = sourceTable + ":" + sourceType;
                        return key;
                    }

                    private void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
                        List<String> columnList = Arrays.asList(sinkColumns.split(","));
                        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
                        entrySet.removeIf(entry -> !columnList.contains(entry.getKey()));
                    }
                }
        );

        // TODO 将流中数据写到kafka的不同主题中
        realDS.print();
        realDS.sinkTo(FlinkSinkUtil.getKafkaSink());
    }
}
