package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTradeProvinceOrderWindowBean;
import com.atguigu.edu.realtime.common.bean.DwsTrafficForSourcePvBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.function.DimAsyncFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Title: DwsTradeProvinceOrderWindow
 * Create on: 2024/12/16
 *
 * @author zhengranran
 * @version 1.0.0
 * Description: 交易域省份粒度下单各窗口汇总表
 * 主要任务： 从 Kafka topic_db 主题读取数据，筛选订单表数据，统计当日各省份下单独立用户数和订单数，
 *          封装为实体类，按照省份 ID 分组，聚合度量字段，补充省份名称信息，将数据写入 Doris。
 * 需要启动的进程：
 *     zk、kafka、maxwell、redis、hdfs、hbase、doris、DwdTradeOrderDetail、DwsTradeProvinceOrderWindow
 */
public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(10033,4,"dws_trade_province_order_window", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //kafkaStrDS.print();
    //TODO 1.对流中数据进行类型转换并过滤空消息   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    //因为订单流中数据是通过左外连接获得，所以会有空值，要过滤
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                        if (StringUtils.isNotEmpty(jsonStr)) {
                            JSONObject jsonObjectDS = JSONObject.parseObject(jsonStr);
                            out.collect(jsonObjectDS);
                        }
                    }
                }
        );
     //TODO 2.按照唯一键(订单明细id)进行分组
        KeyedStream<JSONObject, String> keyByDS = jsonObjDS.keyBy(jsonObject -> jsonObject.getString("id"));
    //TODO 3.去重-状态+抵消
        SingleOutputStreamOperator<JSONObject> distinctDS = keyByDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    //为同一个id内所有数据维护一个单值状态，存的是？？
                    ValueState<JSONObject> lastJsonObjState;

                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //说明数据重复 ，将数据对应的度量值取反向下游传递
                            String origin_amount = lastJsonObj.getString("origin_amount");
                            String coupon_reduce_amount = lastJsonObj.getString("coupon_reduce_amount");
                            String final_amount = lastJsonObj.getString("final_amount");
                            //取反
                            lastJsonObj.put("origin_amount", "-" + origin_amount);
                            lastJsonObj.put("coupon_reduce_amount", "-" + coupon_reduce_amount);
                            lastJsonObj.put("final_amount", "-" + final_amount);
                            collector.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObject);
                        //不为空就直接传下去，并更新到状态中等待10秒钟，看同一id下是否还有数据过来
                        collector.collect(jsonObject);
                    }
                }
        );
        //distinctDS.print();
    //TODO 4.再次对流中数据进行类型转换  jsonObj->实体类对象
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> beanDS = distinctDS.map(
                new MapFunction<JSONObject, DwsTradeProvinceOrderWindowBean>() {
                    @Override
                    public DwsTradeProvinceOrderWindowBean map(JSONObject jsonObject) throws Exception {
                        String provinceId = jsonObject.getString("province_id");
                        String userId = jsonObject.getString("user_id");
                        String orderId = jsonObject.getString("order_id");
                        BigDecimal finalAmount = jsonObject.getBigDecimal("final_amount");
                        Long ts = jsonObject.getLong("ts") * 1000;
                        return DwsTradeProvinceOrderWindowBean.builder()
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                .provinceId(provinceId)
                                .orderTotalAmount(finalAmount)
                                .userId(userId)
                                .ts(ts)
                                .build();
                    }
                }
        );
        //beanDS.print();   //---这里求独立访客
        //使用用户和省份id分组，求独立访客数
        KeyedStream<DwsTradeProvinceOrderWindowBean, Tuple2<String, String>> dimKeyedDS = beanDS.keyBy(
                new KeySelector<DwsTradeProvinceOrderWindowBean, Tuple2<String, String>>() {

                    @Override
                    public Tuple2<String, String> getKey(DwsTradeProvinceOrderWindowBean Bean) throws Exception {
                        return Tuple2.of(Bean.getProvinceId(), Bean.getUserId());
                    }
                }
        );
        //dimKeyedDS.print();

        //todo 统计独立访客状态编程
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> lastVisitDateStateDS = dimKeyedDS.process(
                new KeyedProcessFunction<Tuple2<String, String>, DwsTradeProvinceOrderWindowBean, DwsTradeProvinceOrderWindowBean>() {
                    private ValueState<String> lastVisitDateState;

                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(DwsTradeProvinceOrderWindowBean Bean, KeyedProcessFunction<Tuple2<String, String>, DwsTradeProvinceOrderWindowBean, DwsTradeProvinceOrderWindowBean>.Context context, Collector<DwsTradeProvinceOrderWindowBean> out) throws Exception {
                        //判断是否为当天独立访客
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = Bean.getTs();
                        String curVisitdate = DateFormatUtil.tsToDate(ts);
                        Long uvCount = 0L;
                        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitdate)) {
                            uvCount = 1L;
                            lastVisitDateState.update(curVisitdate);
                        }
                        Bean.setOrderUuCount(uvCount);
                        out.collect(Bean);
                    }
                }
        );

       //lastVisitDateStateDS.print();
        //TODO 5.指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> withWatermarkDS = lastVisitDateStateDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<DwsTradeProvinceOrderWindowBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTradeProvinceOrderWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTradeProvinceOrderWindowBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );
      //withWatermarkDS.print();

    //TODO 6.分组
    KeyedStream<DwsTradeProvinceOrderWindowBean, String> provinceIdKeyedDS
                = withWatermarkDS.keyBy(DwsTradeProvinceOrderWindowBean::getProvinceId);
    //provinceIdKeyedDS.print();

    //TODO 7.开窗
    WindowedStream<DwsTradeProvinceOrderWindowBean, String, TimeWindow> windowDS
                = provinceIdKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

    //TODO 8.聚合
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> reduceDS = windowDS.reduce(
                new ReduceFunction<DwsTradeProvinceOrderWindowBean>() {
                    @Override
                    public DwsTradeProvinceOrderWindowBean reduce(DwsTradeProvinceOrderWindowBean V1, DwsTradeProvinceOrderWindowBean V2) throws Exception {
                        //V1.setOrderCount(V1.getOrderCount() + V2.getOrderCount());
                        V1.setOrderTotalAmount(V1.getOrderTotalAmount().add(V2.getOrderTotalAmount()));
                        V1.getOrderIdSet().addAll(V2.getOrderIdSet());
                        V1.setOrderUuCount(V1.getOrderUuCount() + V2.getOrderUuCount());
                        return V1;
                    }
                },
                new WindowFunction<DwsTradeProvinceOrderWindowBean, DwsTradeProvinceOrderWindowBean, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<DwsTradeProvinceOrderWindowBean> input, Collector<DwsTradeProvinceOrderWindowBean> out) throws Exception {
                        DwsTradeProvinceOrderWindowBean orderBean = input.iterator().next();
                        String sst = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String currentDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(sst);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(currentDate);
                        orderBean.setOrderCount((long)(orderBean.getOrderIdSet().size()));
                        out.collect(orderBean);
                    }
                }
        );
        //reduceDS.print();
     //TODO 9.关联省份维度
        SingleOutputStreamOperator<DwsTradeProvinceOrderWindowBean> withProvinceDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<DwsTradeProvinceOrderWindowBean>() {
                    @Override
                    public void addDims(DwsTradeProvinceOrderWindowBean obj, JSONObject dimJsonObj) {
                        //补充维度属性到流中对象上
                        obj.setProvinceName(dimJsonObj.getString("name"));

                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public String getRowKey(DwsTradeProvinceOrderWindowBean obj) {
                        return obj.getProvinceId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 10.将关联的结果写到Doris
        //withProvinceDS.print();
        withProvinceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));

    }

}
