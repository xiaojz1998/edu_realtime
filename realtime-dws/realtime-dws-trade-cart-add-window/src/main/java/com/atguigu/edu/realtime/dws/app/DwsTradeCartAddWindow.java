package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTradeCartAddWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Title: DwsTradeCartAddWindow
 * Create on: 2024/12/16 17:29
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  交易域加购各窗口汇总表
 *  需要的进程：
 *      zk、kafka、maxwell、doris、DwdBaseDb、DwsTradeCartAddWindow
 */
public class DwsTradeCartAddWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeCartAddWindow().start(
                10027,
                4,
                "dws_trade_cart_add_window",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 类型转换
        SingleOutputStreamOperator<JSONObject> mapDS = kafkaStrDS.map(JSON::parseObject);

        // 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = mapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts") * 1000;
                                    }
                                }
                        )
        );

        // 分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getString("user_id");
                    }
                }
        );
        // 使用状态过滤出独立用户
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> processDS = keyedDS.process(


                new KeyedProcessFunction<String, JSONObject, DwsTradeCartAddWindowBean>() {
                    ValueState<String> lastCartAddState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> lastCartAddStateDesc = new ValueStateDescriptor<>("lastCartAddState", String.class);
                        // 设置状态过期时间
                        lastCartAddStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1l)).build());
                        lastCartAddState = getRuntimeContext().getState(lastCartAddStateDesc);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, DwsTradeCartAddWindowBean>.Context context, Collector<DwsTradeCartAddWindowBean> collector) throws Exception {
                        String lastCartAddDt = lastCartAddState.value();
                        Long ts = jsonObject.getLong("ts") * 1000;
                        String curDate = DateFormatUtil.tsToDate(ts);
                        if (StringUtils.isEmpty(lastCartAddDt) || lastCartAddDt.compareTo(curDate) < 0) {
                            lastCartAddState.update(curDate);
                            DwsTradeCartAddWindowBean result = DwsTradeCartAddWindowBean.builder()
                                    .cartAddUvCount(1L)
                                    .ts(ts)
                                    .build();
                            collector.collect(result);
                        }
                    }
                }
        );
        // 开窗聚合
        SingleOutputStreamOperator<DwsTradeCartAddWindowBean> reduceDS = processDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))


                .reduce(
                        new ReduceFunction<DwsTradeCartAddWindowBean>() {
                            @Override
                            public DwsTradeCartAddWindowBean reduce(DwsTradeCartAddWindowBean dwsTradeCartAddWindowBean, DwsTradeCartAddWindowBean t1) throws Exception {
                                dwsTradeCartAddWindowBean.setCartAddUvCount(dwsTradeCartAddWindowBean.getCartAddUvCount() + t1.getCartAddUvCount());
                                return dwsTradeCartAddWindowBean;
                            }
                        },
                        new AllWindowFunction<DwsTradeCartAddWindowBean, DwsTradeCartAddWindowBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<DwsTradeCartAddWindowBean> iterable, Collector<DwsTradeCartAddWindowBean> collector) throws Exception {
                                String start = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                                String end = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                                String curDate = DateFormatUtil.tsToDate(timeWindow.getStart());
                                DwsTradeCartAddWindowBean next = iterable.iterator().next();
                                next.setEdt(end);
                                next.setStt(start);
                                next.setCurDate(curDate);
                                collector.collect(next);
                            }
                        }
                );
        reduceDS.print("reduceDS");
        reduceDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_window"));
    }
}
