package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTradeOrderWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.TimestampLtz3CompareUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import org.apache.flink.util.Collector;

import javax.print.DocFlavor;
import java.time.Duration;

/**
 * Title: DwsTradePaySucWindow
 * Create on: 2024/12/17 9:54
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  交易域支付成功各窗口汇总表
 *
 */
public class DwsTradePaySucWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradePaySucWindow().start(
                10029,
                4,
                "dws_trade_pay_suc_window",
                Constant.TOPIC_DWD_TRADE_PAY_SUC_DETAIL
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
        //jsonObjDS.print();
        // 水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
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
        // TODO 按照订单明细ID分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(
                new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getString("id");
                    }
                }
        );
        // 使用flink状态去重
        SingleOutputStreamOperator<JSONObject> distinctDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    ValueState<JSONObject> payState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> orderStateDesc = new ValueStateDescriptor<>("order_state", JSONObject.class);
                        // 设置过期时间
                        orderStateDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.minutes(1L)).build());
                        payState = getRuntimeContext().getState(orderStateDesc);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject orderStateObj = payState.value();
                        if (orderStateObj == null) {
                            // 状态为空  说明是第一条数据
                            //启动定时器
                            context.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000L);
                            payState.update(jsonObj);
                        } else {
                            String stateTs = orderStateObj.getString("row_op_ts");
                            String curTs = jsonObj.getString("row_op_ts");
                            // 如果当前的时间大于状态中的时间  说明当前的数据更加新一些
                            // 更新数据
                            if (TimestampLtz3CompareUtil.compare(curTs, stateTs) >= 0) {
                                payState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        //定时器触发的时候，执行的方法
                        JSONObject jsonObj = payState.value();
                        out.collect(jsonObj);
                        //清状态里的数据
                        payState.clear();
                    }
                }
        );

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
    }
}
