package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.baseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Title: DwdTrafficUserJumpDetail
 * Create on: 2024/12/15
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *  流量域用户跳出事务事实表  任务：过滤用户跳出明细数据  一条数据代表一次跳出
 *  需要启动的进程：
 *     zk,kf,flume,DwdBaseLog,DwdTrafficUserJumpDetail
 */
public class DwdTrafficUserJumpDetail extends baseApp {
    public static void main(String[] args) {
        new DwdTrafficUserJumpDetail().start(10013,4,"dwd_traffic_user_jump_detail", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //kafkaStrDS.print();
        //TODO 1.对流中数据进行类型转换   ---jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> pageJsonObjDS = kafkaStrDS.map(jsonStr -> JSONObject.parseObject(jsonStr));
        //TODO 2 添加水位线        ---为了开窗
        SingleOutputStreamOperator<JSONObject> WatermarkDS = pageJsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                }
                        ));

        //TODO 3.按照Mid分组
        KeyedStream<JSONObject, String> keyByjsonObjDS
                = WatermarkDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        //TODO 4 定义cep匹配规则

        //TODO 5 将CEP作用到流上
        //TODO 6 提取匹配数据和超时数据
        //TODO 7 合并数据写出到kafka





    }
}
