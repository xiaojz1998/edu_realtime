package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTrafficForSourcePvBean;
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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Title: DwsTrafficVcChArIsNewPageViewWindow
 * Create on: 2024/12/16
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *   本程序的任务：本节汇总表中需要有会话数、页面浏览数、浏览总时长、独立访客数、跳出会话数五个度量字段
 *   需要启动的进程
 *      zk、kafka、flume、doris、DwdBaseLog、DwsTrafficVcChArVisitorCategoryPageViewWindow
 *  太麻烦了，就在日志流里分
 */
public class DwsTrafficVcChArVisitorCategoryPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArVisitorCategoryPageViewWindow().start(
                10022,
                4,
                "dws_traffic_vc_ch_ar_visitor_category_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        //TODO 1.对流中数据进行类型转换    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        //TODO 2.按照mid进行分组
        KeyedStream<JSONObject, String> midKeyedDS
                = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));


        //TODO 4.jsonObj->实体类对象   使用Flink状态编程判断是否为独立访客  相当于WordCount的封装二元组
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> beanDS = midKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, DwsTrafficForSourcePvBean>() {
                    private ValueState<String> lastVisitDateState;

                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, DwsTrafficForSourcePvBean>.Context context, Collector<DwsTrafficForSourcePvBean> out) throws Exception {
                        JSONObject commonJsonObj = jsonObject.getJSONObject("common");
                        String versionCode = commonJsonObj.getString("vc");
                        String ar = commonJsonObj.getString("ar");
                        String sourceId = commonJsonObj.getString("sc");
                        String isNew = commonJsonObj.getString("is_new");

                        JSONObject pageJsonObj = jsonObject.getJSONObject("page");
                        //判断是否为当天独立访客
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObject.getLong("ts");
                        String curVisitdate = DateFormatUtil.tsToDate(ts);
                        Long uvCount = 0L;
                        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitdate)) {
                            uvCount = 1L;
                            lastVisitDateState.update(curVisitdate);
                        }
                        //会话计数
                        String lastPageId = pageJsonObj.getString("last_page_id");
                        Long totalSessionCount = StringUtils.isEmpty(lastPageId) ? 1L : 0L;

                        DwsTrafficForSourcePvBean trafficForSourcePvBean = new DwsTrafficForSourcePvBean(
                                "",
                                "",
                                "",
                                versionCode,
                                sourceId,
                                "",
                                ar,
                                "",
                                isNew,
                                uvCount,
                                totalSessionCount,
                                1L,
                                pageJsonObj.getLong("during_time"),
                                ts
                        );
                        out.collect(trafficForSourcePvBean);
                    }
                }
        );
        //beanDS.print();
        //TODO 5.指定Watermark生成策略以及提取事件时间字段
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsTrafficForSourcePvBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsTrafficForSourcePvBean>() {
                                    @Override
                                    public long extractTimestamp(DwsTrafficForSourcePvBean dwsTrafficForSourcePvBean, long l) {
                                        return dwsTrafficForSourcePvBean.getTs();
                                    }
                                }
                        )
        );
        //TODO 6.按照统计的维度进行分组
        KeyedStream<DwsTrafficForSourcePvBean, Tuple4<String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<DwsTrafficForSourcePvBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(DwsTrafficForSourcePvBean bean) throws Exception {
                        return Tuple4.of(
                                bean.getAr(),
                                bean.getVersionCode(),
                                bean.getSourceId(),
                                bean.getIsNew());
                    }
                }
        );
        //TODO 7.开窗
        WindowedStream<DwsTrafficForSourcePvBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 8.聚合
        SingleOutputStreamOperator<DwsTrafficForSourcePvBean> reduceDS = windowDS.reduce(
                new ReduceFunction<DwsTrafficForSourcePvBean>() {
                    @Override
                    public DwsTrafficForSourcePvBean reduce(DwsTrafficForSourcePvBean v1, DwsTrafficForSourcePvBean v2) throws Exception {
                        v1.setPageViewCount(v1.getPageViewCount() + v2.getPageViewCount());
                        v1.setUvCount(v1.getUvCount() + v2.getUvCount());
                        v1.setTotalDuringTime(v1.getTotalDuringTime() + v2.getTotalDuringTime());
                        v1.setTotalSessionCount(v1.getTotalSessionCount() + v2.getTotalSessionCount());
                        return v1;
                    }
                },
                new WindowFunction<DwsTrafficForSourcePvBean, DwsTrafficForSourcePvBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> tuple4, TimeWindow window, Iterable<DwsTrafficForSourcePvBean> input, Collector<DwsTrafficForSourcePvBean> out) throws Exception {
                        DwsTrafficForSourcePvBean viewBean = input.iterator().next();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());  //开窗取到时间，赋值给bean
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCur_date(curDate);
                        out.collect(viewBean);
                    }
                }
        );
        //TODO 9.将聚合结果写到Doris中
        reduceDS.print();

        reduceDS
                //将流中实体类对象转换为json格式字符串
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_visitor_category_page_view_window"));

    }
}
