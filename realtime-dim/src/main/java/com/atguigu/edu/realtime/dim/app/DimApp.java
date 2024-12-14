package com.atguigu.edu.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.bean.TableProcess;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import com.atguigu.edu.realtime.common.util.HBaseUtil;
import com.atguigu.edu.realtime.dim.function.DimSinkFunction;
import com.atguigu.edu.realtime.dim.function.TableProcessFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Title: DimApp
 * Create on: 2024/12/14 1:55
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  Dim层开发
 *  需要启动的进程:
 *    zk、kafka、maxwell、hdfs、hbase、DimApp
 */
public class DimApp {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8088);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        /*

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //2.2 设置检查点超时时间
        checkpointConfig.setCheckpointTimeout(60000L);
        //2.3 设置job取消之后检查点是否保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.7 设置检查点存储路径
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");
        //2.8 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");

         */
        //TODO 3.从kafka主题中读取主流业务数据
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");
        //3.3 消费数据 封装为流
        DataStreamSource<String> kafkaSourceDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        //kafkaSourceDS.print();

        // kafka 数据格式
        //{"database":"edu","table":"test_exam_question","type":"insert","ts":1734144588,
        // "xid":187609,"commit":true,"data":{"id":63576,"exam_id":4310,"paper_id":204,
        //  "question_id":1977,"user_id":21,"answer":"","is_correct":"0","score":0.00,
        //   "create_time":"2024-12-14 10:49:48","update_time":null,"deleted":"0"}}
        // TODO 对流中数据进行类型转换并进行简单的ETL    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSourceDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject jsonObject = JSON.parseObject(s);
                            String db = jsonObject.getString("database");
                            String type = jsonObject.getString("type");
                            String data = jsonObject.getString("data");
                            if ("edu".equals(db)
                                    && ("insert".equals(type)
                                    || "update".equals(type)
                                    || "delete".equals(type)
                                    || "bootstrap-insert".equals(type))
                                    && data != null
                                    && data.length() > 2
                            ) {
                                collector.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            System.out.println("不是一个标准的json");
                        }
                    }
                }
        );
        //jsonObjDS.print("jsonObjDS");


        //TODO 5.使用FlinkCDC读取配置表中的配置信息
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("table_process");
        //"op":"r" {"before":null,"after":{"source_table":"financial_sku_cost","sink_table":"dim_financial_sku_cost","sink_family":"info","sink_columns":"id,sku_id,sku_name,busi_date,is_lastest,sku_cost,create_time","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1732870578960,"transaction":null}
        //"op":"c" {"before":null,"after":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732870643000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11499579,"row":0,"thread":14,"query":null},"op":"c","ts_ms":1732870642929,"transaction":null}
        //"op":"u" {"before":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name","sink_row_key":"id"},"after":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name,age","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732870709000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11499941,"row":0,"thread":14,"query":null},"op":"u","ts_ms":1732870709291,"transaction":null}
        //"op":"d" {"before":{"source_table":"t_a","sink_table":"dim_a","sink_family":"info","sink_columns":"id,name,age","sink_row_key":"id"},"after":null,"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1732870748000,"snapshot":"false","db":"gmall0620_config","sequence":null,"table":"table_process_dim","server_id":1,"gtid":null,"file":"mysql-bin.000002","pos":11500331,"row":0,"thread":14,"query":null},"op":"d","ts_ms":1732870748451,"transaction":null}
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
        //mysqlStrDS.print("mysqlStrDS");
        // TODO 对流中数据进行类型转换   jsonStr->实体类对象
        SingleOutputStreamOperator<TableProcess> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcess>() {

                    @Override
                    public TableProcess map(String s) throws Exception {
                        //为了处理方便，将jsonStr转换为jsonObj
                        JSONObject jsonObject = JSON.parseObject(s);
                        //获取对配置表进行的操作的类型
                        String op = jsonObject.getString("op");
                        TableProcess tableProcess = null;
                        if ("d".equals(op)) {
                            //对配置表进行了删除操作   从before属性中获取删除前的信息
                            tableProcess = jsonObject.getObject("before", TableProcess.class);
                        } else {
                            //对配置表进行了读取、添加、更新操作  从after属性中获取最新的配置信息
                            tableProcess = jsonObject.getObject("after", TableProcess.class);
                        }
                        // 补充操作类型
                        tableProcess.setOp(op);
                        tableProcess.setSinkFamily("info");
                        return tableProcess;
                    }
                }
        ).setParallelism(1);
        //tpDS.print("tpDS");
        //TODO 6.根据配置表中的配置信息在HBase中执行建表或者删表操作
        tpDS = tpDS.map(
                new RichMapFunction<TableProcess, TableProcess>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcess map(TableProcess tp) throws Exception {
                        //获取对配置表进行操作的类型
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] families = tp.getSinkFamily().split(",");
                        if ("r".equals(op) || "c".equals(op)) {
                            //从配置表中读取一条数据或者向配置表中添加一条配置  执行建表操作
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, families);
                        } else if ("d".equals(op)) {
                            //从配置表中删除了一条配置  执行删表操作
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                        } else {
                            //对配置表中的某条数据进行了更新操作  先删表再建表
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, families);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);
//        tpDS.print("tpDS");

        //TODO 广播配置流---broadcast  将主流和广播流进行关联---connect   对关联后的数据进行处理
        MapStateDescriptor<String, TableProcess> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcess>("mapStateDescriptor",String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, TableProcess> connectDS = jsonObjDS.connect(broadcastDS);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dimDS = connectDS.process(
                new TableProcessFunction(mapStateDescriptor)
        );
        //dimDS.print("dimDS");

        dimDS.addSink(
                new DimSinkFunction()
        );
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
