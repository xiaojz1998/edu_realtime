package com.atguigu.edu.realtime.dwd.db.split;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.baseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Title: DwdBaseLog
 * Create on: 2024/12/14
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 * 日志分流
 * 需要启动的进程
 *      zk、kafka、flume、DwdBaseLog
 */

public class DwdBaseLog extends baseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 1.对流中数据进行类型转换并ETL  jsonStr->jsonObj  如果是脏数据放到侧流中--->kafka
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                //1.1 定义侧输出流标签
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                        try {
                            //1.2 etl（判断从kafka消费到的数据是否为标准json）
                            //调用方法，将Jsonstr转化成Jsonobj
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            //如果发生了异常，说明不是标准的json，将其放到侧流
                            System.out.println("----------------输入的jison格式不正确-----------");
                            context.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        //jsonObjDS.print("标准");
        //TODO 2.新老访客标记修复
        //2.1 按照设备id进行分组
        KeyedStream<JSONObject, String> keyByDS = jsonObjDS.keyBy(
                JSONObject -> JSONObject.getJSONObject("common").getString("mid")
        );
        //2.2 使用Flink的状态编程修复新老访客标记
        SingleOutputStreamOperator<JSONObject> fixedDS = keyByDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> lastVisitState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //为每一个mid维护一个时间状态，状态不失效
                        ValueStateDescriptor lastValueStateDesc = new ValueStateDescriptor<>("lastValueStateDesc", String.class);
                        lastVisitState = getRuntimeContext().getState(lastValueStateDesc);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        String VisitDate = lastVisitState.value();
                        Long ts = jsonObject.getLong("ts");
                        //毫秒转日期
                        String currentDate = DateFormatUtil.tsToDateTime(ts);
                        if ("1".equals(isNew)) {
                            //is_new = 1
                            //取状态，为null，ts更新到状态中；不为空，且不是今天，将is_new改为0；不为空，首次日期是当日，不操作（24h）
                            if (StringUtils.isEmpty(VisitDate)) {
                                lastVisitState.update(currentDate);
                            } else {
                                if (!VisitDate.equals(currentDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            //is_new = 0
                            //取状态，为null，状态丢失，更新为昨天；不为null不操作
                            if (StringUtils.isEmpty(VisitDate)) {
                                String yesterDay = DateFormatUtil.tsToDateTime(ts - 24 * 60 * 60 * 1000);
                                lastVisitState.update(yesterDay);
                            }
                        }
                        out.collect(jsonObject);
                    }
                }
        );
        //fixedDS.print();
        //TODO 3.分流    错误日志-错误侧输出流  启动日志-启动侧输出流 播放日期-播放侧输出流  曝光日志-曝光侧输出流  动作日志-动作侧输出流  页面日志-主流
        //3.1 定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag"){};
        OutputTag<String> startTag = new OutputTag<String>("startTag"){};
        OutputTag<String> displaysTag = new OutputTag<String>("displaysrTag"){};
        OutputTag<String> actionsTag = new OutputTag<String>("actionsTag"){};
        OutputTag<String> appVideoTag = new OutputTag<String>("appVideoTag"){};
        //3.2 分流
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {

                        JSONObject errJsonObject = jsonObject.getJSONObject("err");
                        JSONObject startJsonObject = jsonObject.getJSONObject("start");
                        JSONObject pageJsonObject = jsonObject.getJSONObject("page");
                        JSONObject appVideoJsonObject = jsonObject.getJSONObject("appVideo");
                        //错误日志
                        if (errJsonObject != null) {
                            //将错误日志放到错误侧输出流
                            context.output(errTag, jsonObject.toJSONString());
                            jsonObject.remove("err"); //将错误日志移除
                        }
                        // 启动日志
                        if (startJsonObject != null) {
                            context.output(startTag, jsonObject.toJSONString());
                        }
                        //播放日志
                        if (appVideoJsonObject != null) {
                            context.output(appVideoTag, jsonObject.toJSONString());
                        }
                        //页面日志
                        if (pageJsonObject != null) {
                            //过滤页面日志所需字段page common ts
                            JSONObject commonJsonObject = jsonObject.getJSONObject("common");
                            Long ts = jsonObject.getLong("ts");
                            JSONArray displayArr = pageJsonObject.getJSONArray("display");
                            if (displayArr != null && displayArr.size() > 0) { //双重判断：displayArr可能是空的集合或数组，可以防止空指针异常
                                //曝光日志
                                //遍历曝光数据
                                for (int i = 0; i < displayArr.size(); i++) { // displays是由一个或多个 JSON 对象组成的数组。
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                    //创建一个新的json对象，用于封装每一条曝光数据
                                    JSONObject newDisplayJson = new JSONObject();
                                    newDisplayJson.put("common", commonJsonObject);//common
                                    newDisplayJson.put("page", pageJsonObject);//page里是json对象
                                    newDisplayJson.put("display", displayJsonObj);//display数组中一个一个的元素
                                    newDisplayJson.put("ts", ts);
                                    //放到曝光侧输出流中
                                    context.output(displaysTag, newDisplayJson.toJSONString());
                                }
                                jsonObject.remove("display");//将曝光数据去除
                            }
                            //动作日志
                            JSONArray actionsArr = pageJsonObject.getJSONArray("actions");
                            if (actionsArr != null && actionsArr.size() > 0) {
                                for (int i = 0; i < actionsArr.size(); i++) {
                                    JSONObject actionsObj = actionsArr.getJSONObject(i); //得到数组中每一个元素
                                    JSONObject newActionsObj = new JSONObject();
                                    newActionsObj.put("common", commonJsonObject);
                                    newActionsObj.put("page", pageJsonObject);
                                    newActionsObj.put("action", actionsObj);
                                    newActionsObj.put("ts", ts);
                                    context.output(actionsTag, newActionsObj.toJSONString());
                                }
                                jsonObject.remove("actions");
                            }
                            //页面日志放到主流中
                            collector.collect(jsonObject.toJSONString());
                        }

                    }
                }
        );
        SideOutputDataStream<String>  errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String>  startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String>  displayDS = pageDS.getSideOutput(displaysTag);
        SideOutputDataStream<String>  actionDS = pageDS.getSideOutput(actionsTag);
        SideOutputDataStream<String>  appVideoDS = pageDS.getSideOutput(appVideoTag);

        pageDS.print("日志分流之page流");
        errDS.print("日志分流之err流");
        startDS.print("日志分流之start流");
        displayDS.print("日志分流之display流");
        actionDS.print("日志分流之action流");
        appVideoDS.print("日志分流之appVideo流");
        //3.3 将不同流的数据写到kafka不同主题中
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        appVideoDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_APPVIDEO));
    }
}
