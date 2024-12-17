package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTradeOrderWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.BeanToJsonStrMapFunction;
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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Title: DwsTradeOrderWindow
 * Create on: 2024/12/16 20:53
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *   交易域下单各窗口汇总表
 *      zk kafka flume maxwell DwdBaseLog DwdTradeOrderDetail DwsTradeOrderWindow
 *
 *
 */
public class DwsTradeOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(
                10028,
                4,
                "dws_trade_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 转换数据结构 过滤null
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
        // 按照唯一键进行分组
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
        //distinctDS.print("去重后数据：");
        // TODO  按照user_id分组
        KeyedStream<JSONObject, String> keyedUserIdDS = distinctDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getString("user_id");
                    }
                }
        );
        // 判断是否为独立用户
        SingleOutputStreamOperator<DwsTradeOrderWindowBean> uvCountStream = keyedUserIdDS.process(new KeyedProcessFunction<String, JSONObject, DwsTradeOrderWindowBean>() {
            ValueState<String> lastOrderDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_order_dt_state", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<DwsTradeOrderWindowBean> out) throws Exception {
                long ts = jsonObj.getLong("ts") * 1000;
                String curDate = DateFormatUtil.tsToDate(ts);
                String lastOrderDt = lastOrderDtState.value();
                System.out.println("lastOrderDt:"+lastOrderDt+" curDate:"+curDate);
                long orderUvCount = 0L;
                long newOrderUserCount = 0L;
                if (StringUtils.isEmpty(lastOrderDt)) {
                    // 是新用户
                    orderUvCount = 1L;
                    newOrderUserCount = 1L;
                    lastOrderDtState.update(curDate);
                } else if (lastOrderDt.compareTo(curDate) < 0) {

                    orderUvCount = 1L;
                }
                // 判断是独立用户才需要往下游传递
                if (orderUvCount != 0) {
                    out.collect(DwsTradeOrderWindowBean.builder()
                            .orderUvCount(orderUvCount)
                            .newOrderUserCount(newOrderUserCount)
                            .ts(ts)
                            .build());
                }
            }
        });
        SingleOutputStreamOperator<DwsTradeOrderWindowBean> withWatermarkDS = uvCountStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeOrderWindowBean>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTradeOrderWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTradeOrderWindowBean dwsTradeOrderWindowBean, long l) {
                                        return dwsTradeOrderWindowBean.getTs();
                                    }
                                }
                        )
        );

        // 开窗聚合
        SingleOutputStreamOperator<DwsTradeOrderWindowBean> reduceDS = withWatermarkDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10l)))
                .reduce(
                        new ReduceFunction<DwsTradeOrderWindowBean>() {
                            @Override
                            public DwsTradeOrderWindowBean reduce(DwsTradeOrderWindowBean dwsTradeOrderWindowBean, DwsTradeOrderWindowBean t1) throws Exception {
                                dwsTradeOrderWindowBean.setOrderUvCount(dwsTradeOrderWindowBean.getOrderUvCount() + t1.getOrderUvCount());
                                dwsTradeOrderWindowBean.setNewOrderUserCount(dwsTradeOrderWindowBean.getNewOrderUserCount() + t1.getNewOrderUserCount());
                                return dwsTradeOrderWindowBean;
                            }
                        },
                        new ProcessAllWindowFunction<DwsTradeOrderWindowBean, DwsTradeOrderWindowBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<DwsTradeOrderWindowBean, DwsTradeOrderWindowBean, TimeWindow>.Context context, Iterable<DwsTradeOrderWindowBean> iterable, Collector<DwsTradeOrderWindowBean> collector) throws Exception {
                                String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                String curDate = DateFormatUtil.tsToDate(context.window().getStart());
                                DwsTradeOrderWindowBean next = iterable.iterator().next();
                                next.setStt(start);
                                next.setEdt(end);
                                next.setCurDate(curDate);
                                collector.collect(next);

                            }
                        }
                );

        reduceDS.print("订单统计：");
        reduceDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_order_window"));


    }
}
