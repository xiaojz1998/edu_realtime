package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTradeOrderWindowBean;
import com.atguigu.edu.realtime.common.bean.DwsTradeSourceOrderWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.function.DimAsyncFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import com.atguigu.edu.realtime.common.util.TimestampLtz3CompareUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
 * Title: DwsTradeSourceOrderWindow
 * Create on: 2024/12/17 15:03
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  交易域来源粒度下单各窗口汇总表
 *  需要进程：
 *      zk kafka maxwell flume DwdBaseLog DwdTradeOrderDetail  DwsTradeSourceOrderWindow
 */
public class DwsTradeSourceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSourceOrderWindow().start(
                10031,
                4,
                "dws_trade_source_order_window",
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
                                if(!StringUtils.isEmpty(jsonObject.getString("source_id"))){
                                    collector.collect(jsonObject);
                                }
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
        // TODO  按照来源id和用户id分组
        KeyedStream<JSONObject, String> keyedUSDS = distinctDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getString("user_id") + jsonObject.getString("source_id");
                    }
                }
        );
        // TODO 6 统计独立用户
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> uuBeanStream = keyedUSDS.process(new KeyedProcessFunction<String, JSONObject, DwsTradeSourceOrderWindowBean>() {

            ValueState<String> lastOrderDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastOrderDtStateDesc = new ValueStateDescriptor<>("last_order_dt_state", String.class);
                lastOrderDtStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                lastOrderDtState = getRuntimeContext().getState(lastOrderDtStateDesc);
            }

            @Override
            public void processElement(JSONObject jsonObj, Context ctx, Collector<DwsTradeSourceOrderWindowBean> out) throws Exception {
                long ts = jsonObj.getLong("ts") * 1000;
                String lastOrderDt = lastOrderDtState.value();
                String curDt = DateFormatUtil.tsToDate(ts);
                Long orderUuCount = 0L;
                if (StringUtils.isEmpty(lastOrderDt) || lastOrderDt.compareTo(curDt) < 0) {
                    // 判断为独立用户
                    orderUuCount = 1L;
                    lastOrderDtState.update(curDt);
                }
                out.collect(DwsTradeSourceOrderWindowBean.builder()
                        .sourceId(jsonObj.getString("source_id"))
                        .orderTotalAmount(Double.parseDouble(jsonObj.getString("final_amount")))
                        .orderUuCount(orderUuCount)
                        .orderId(jsonObj.getString("order_id"))
                        .ts(ts)
                        .build());
            }
        });

        //uuBeanStream.print("uuBeanStream");


        // TODO 按照订单id分组
        KeyedStream<DwsTradeSourceOrderWindowBean, String> keyedOrderIdDS = uuBeanStream.keyBy(
                new KeySelector<DwsTradeSourceOrderWindowBean, String>() {
                    @Override
                    public String getKey(DwsTradeSourceOrderWindowBean dwsTradeSourceOrderWindowBean) throws Exception {
                        return dwsTradeSourceOrderWindowBean.getOrderId();
                    }
                }
        );


        // TODO 统计订单数量
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> orderBeanDS = keyedOrderIdDS.process(
                new KeyedProcessFunction<String, DwsTradeSourceOrderWindowBean, DwsTradeSourceOrderWindowBean>() {
                    ValueState<String> lastOrderIdState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastOrderStateDesc = new ValueStateDescriptor<>("last_order_state", String.class);
                        lastOrderStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(1)).build());
                        lastOrderIdState = getRuntimeContext().getState(lastOrderStateDesc);
                    }

                    @Override
                    public void processElement(DwsTradeSourceOrderWindowBean dwsTradeSourceOrderWindowBean, KeyedProcessFunction<String, DwsTradeSourceOrderWindowBean, DwsTradeSourceOrderWindowBean>.Context context, Collector<DwsTradeSourceOrderWindowBean> collector) throws Exception {
                        String lastOrderId = lastOrderIdState.value();
                        Long orderCount = 0L;
                        String orderId = dwsTradeSourceOrderWindowBean.getOrderId();
                        if (StringUtils.isEmpty(lastOrderId)) {
                            orderCount = 1L;
                            lastOrderIdState.update(orderId);
                        }
                        dwsTradeSourceOrderWindowBean.setOrderCount(orderCount);
                        collector.collect(dwsTradeSourceOrderWindowBean);
                    }
                }
        );
        //orderBeanDS.print("orderBeanDS");


        // 设置水位线
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> withWatermarkDS = orderBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTradeSourceOrderWindowBean>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTradeSourceOrderWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTradeSourceOrderWindowBean dwsTradeSourceOrderWindowBean, long l) {
                                        return dwsTradeSourceOrderWindowBean.getTs();
                                    }
                                }
                        )
        );
        // 分组开窗聚合
        KeyedStream<DwsTradeSourceOrderWindowBean, String> keyedSourceIdDS = withWatermarkDS.keyBy(
                new KeySelector<DwsTradeSourceOrderWindowBean, String>() {
                    @Override
                    public String getKey(DwsTradeSourceOrderWindowBean dwsTradeSourceOrderWindowBean) throws Exception {
                        return dwsTradeSourceOrderWindowBean.getSourceId();
                    }
                }
        );
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> reduceDS = keyedSourceIdDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(
                        new ReduceFunction<DwsTradeSourceOrderWindowBean>() {
                            @Override
                            public DwsTradeSourceOrderWindowBean reduce(DwsTradeSourceOrderWindowBean dwsTradeSourceOrderWindowBean, DwsTradeSourceOrderWindowBean t1) throws Exception {
                                dwsTradeSourceOrderWindowBean.setOrderTotalAmount(dwsTradeSourceOrderWindowBean.getOrderTotalAmount() + t1.getOrderTotalAmount());
                                dwsTradeSourceOrderWindowBean.setOrderCount(dwsTradeSourceOrderWindowBean.getOrderCount() + t1.getOrderCount());
                                dwsTradeSourceOrderWindowBean.setOrderUuCount(dwsTradeSourceOrderWindowBean.getOrderUuCount() + t1.getOrderUuCount());
                                return dwsTradeSourceOrderWindowBean;
                            }
                        },
                        new ProcessWindowFunction<DwsTradeSourceOrderWindowBean, DwsTradeSourceOrderWindowBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<DwsTradeSourceOrderWindowBean, DwsTradeSourceOrderWindowBean, String, TimeWindow>.Context context, Iterable<DwsTradeSourceOrderWindowBean> iterable, Collector<DwsTradeSourceOrderWindowBean> collector) throws Exception {
                                String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                String curDate = DateFormatUtil.tsToDate(context.window().getStart());
                                DwsTradeSourceOrderWindowBean next = iterable.iterator().next();
                                next.setStt(start);
                                next.setEdt(end);
                                next.setCurDate(curDate);
                                collector.collect(next);
                            }
                        }
                );

        // TODO 关联来源名称
        SingleOutputStreamOperator<DwsTradeSourceOrderWindowBean> withSourceNameDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<DwsTradeSourceOrderWindowBean>() {

                    @Override
                    public void addDims(DwsTradeSourceOrderWindowBean obj, JSONObject dimJsonObj) {
                        obj.setSourceName(dimJsonObj.getString("source_site"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_source";
                    }

                    @Override
                    public String getRowKey(DwsTradeSourceOrderWindowBean obj) {
                        return obj.getSourceId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withSourceNameDS.print(" withSourceNameDS");
        withSourceNameDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_source_order_window"));
    }
}
