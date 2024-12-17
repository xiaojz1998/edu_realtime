package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTradeCourseOrderWindowBean;
import com.atguigu.edu.realtime.common.bean.DwsTradeOrderWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.function.DimAsyncFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import com.atguigu.edu.realtime.common.util.TimestampLtz3CompareUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Title: DwsTradeCourseOrderWindow
 * Create on: 2024/12/17 11:28
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  交易域课程粒度下单各窗口汇总表
 *  需要的进程：
 *      zk kafka flume maxwell DwdBaseLog DwdTradeOrderDetail
 */
public class DwsTradeCourseOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeCourseOrderWindow().start(
                10030,
                4,
                "dws_trade_course_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 过滤null
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            if (!StringUtils.isEmpty(s)) {
                                JSONObject jsonObject = JSONObject.parseObject(s);
                                collector.collect(jsonObject);
                            }
                        } catch (Exception e) {
                            System.out.println("不是一个标准json");
                        }
                    }
                }
        );
        // 按照id键进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getString("id");
                    }
                }
        );
        // 去重
        SingleOutputStreamOperator<JSONObject> distinctDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<JSONObject> orderState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> orderStateDesc = new ValueStateDescriptor<>("order_state", JSONObject.class);
                        // 设置过期时间
                        orderStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(1L)).build());
                        orderState = getRuntimeContext().getState(orderStateDesc);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject orderStateObj = orderState.value();
                        if (orderStateObj == null) {
                            // 状态为空  说明是第一条数据
                            //启动定时器
                            context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000L);
                            orderState.update(jsonObj);
                        } else {
                            String stateTs = orderStateObj.getString("row_op_ts");
                            String curTs = jsonObj.getString("row_op_ts");
                            // 如果当前的时间大于状态中的时间  说明当前的数据更加新一些
                            // 更新数据
                            if (TimestampLtz3CompareUtil.compare(curTs, stateTs) >= 0) {
                                orderState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject value = orderState.value();
                        out.collect(value);
                        orderState.clear();
                    }
                }
        );
        // 转换结构为bean
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> mapDS = distinctDS.map(
                new MapFunction<JSONObject, DwsTradeCourseOrderWindowBean>() {
                    @Override
                    public DwsTradeCourseOrderWindowBean map(JSONObject jsonObject) throws Exception {
                        DwsTradeCourseOrderWindowBean build = DwsTradeCourseOrderWindowBean.builder()
                                .courseId(jsonObject.getString("course_id"))
                                .orderTotalAmount(jsonObject.getDouble("final_amount"))
                                .ts(jsonObject.getLong("ts") * 1000)
                                .build();

                        return build;

                    }
                }
        );
        //mapDS.print("mapDS:");
        // TODO 添加水位线
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> withWatermarkDS = mapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeCourseOrderWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<DwsTradeCourseOrderWindowBean>() {
                            @Override
                            public long extractTimestamp(DwsTradeCourseOrderWindowBean dwsTradeCourseOrderWindowBean, long l) {
                                return dwsTradeCourseOrderWindowBean.getTs();
                            }
                        })
        );
        //TODO 按照课程ID分组
        KeyedStream<DwsTradeCourseOrderWindowBean, String> keyedCourseIdDS = withWatermarkDS.keyBy(
                new KeySelector<DwsTradeCourseOrderWindowBean, String>() {
                    @Override
                    public String getKey(DwsTradeCourseOrderWindowBean dwsTradeCourseOrderWindowBean) throws Exception {
                        return dwsTradeCourseOrderWindowBean.getCourseId();
                    }
                }
        );

        // 开窗聚合
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> reduceDS = keyedCourseIdDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10l)))
                .reduce(
                        new ReduceFunction<DwsTradeCourseOrderWindowBean>() {
                            @Override
                            public DwsTradeCourseOrderWindowBean reduce(DwsTradeCourseOrderWindowBean dwsTradeCourseOrderWindowBean, DwsTradeCourseOrderWindowBean t1) throws Exception {
                                dwsTradeCourseOrderWindowBean.setOrderTotalAmount(dwsTradeCourseOrderWindowBean.getOrderTotalAmount() + t1.getOrderTotalAmount());
                                return dwsTradeCourseOrderWindowBean;
                            }
                        },
                        new ProcessWindowFunction<DwsTradeCourseOrderWindowBean, DwsTradeCourseOrderWindowBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<DwsTradeCourseOrderWindowBean, DwsTradeCourseOrderWindowBean, String, TimeWindow>.Context context, Iterable<DwsTradeCourseOrderWindowBean> iterable, Collector<DwsTradeCourseOrderWindowBean> collector) throws Exception {
                                String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                String curDate = DateFormatUtil.tsToDate(context.window().getStart());
                                DwsTradeCourseOrderWindowBean next = iterable.iterator().next();
                                next.setStt(start);
                                next.setEdt(end);
                                next.setCurDate(curDate);
                                collector.collect(next);
                            }
                        }
                );

        //reduceDS.print("reduceDS:");
        // 维度关联
        // 关联课程名称以及科目id

        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> withCourseNameDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<DwsTradeCourseOrderWindowBean>() {
                    @Override
                    public void addDims(DwsTradeCourseOrderWindowBean obj, JSONObject dimJsonObj) {
                        obj.setCourseName(dimJsonObj.getString("course_name"));
                        obj.setSubjectId(dimJsonObj.getString("subject_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_course_info";
                    }

                    @Override
                    public String getRowKey(DwsTradeCourseOrderWindowBean obj) {
                        return obj.getCourseId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //withCourseNameDS.print("withCourseNameDS");

        // 关联科目表
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> withSubjectNameDS = AsyncDataStream.unorderedWait(
                withCourseNameDS,
                new DimAsyncFunction<DwsTradeCourseOrderWindowBean>() {
                    @Override
                    public void addDims(DwsTradeCourseOrderWindowBean obj, JSONObject dimJsonObj) {
                        obj.setSubjectName(dimJsonObj.getString("subject_name"));
                        obj.setCategoryId(dimJsonObj.getString("category_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_subject_info";
                    }

                    @Override
                    public String getRowKey(DwsTradeCourseOrderWindowBean obj) {
                        return obj.getSubjectId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withSubjectNameDS.print("withSubjectNameDS");

        // 关联类别表
        SingleOutputStreamOperator<DwsTradeCourseOrderWindowBean> withCategoryNameDS = AsyncDataStream.unorderedWait(
                withSubjectNameDS,
                new DimAsyncFunction<DwsTradeCourseOrderWindowBean>() {
                    @Override
                    public void addDims(DwsTradeCourseOrderWindowBean obj, JSONObject dimJsonObj) {
                        obj.setCategoryName(dimJsonObj.getString("category_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category_info";
                    }

                    @Override
                    public String getRowKey(DwsTradeCourseOrderWindowBean obj) {
                        return obj.getCategoryId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withCategoryNameDS.print("withCategoryNameDS");
        withSubjectNameDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_course_order_window"));



    }
}
