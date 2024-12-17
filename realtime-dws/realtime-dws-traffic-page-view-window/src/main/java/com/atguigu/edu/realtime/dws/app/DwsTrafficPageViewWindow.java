package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsTrafficPageViewWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Title: DwsTrafficPageViewWindow
 * Create on: 2024/12/16
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *     主要任务：从 Kafka 页面日志主题读取数据，统计当日的首页、课程列表页和课程详情页独立访客数。
 *     启动进程：zk、kafka、flume、doris、DwdBaseLog、DwsTrafficPageViewWindow
 *
 */
public class DwsTrafficPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficPageViewWindow().start(10023,4, "dws_traffic_home_course_detail_page_view_window",Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //kafkaStrDS.print();  数据格式正确
        //TODO 1.读取 Kafka 页面主题数据,对流中数据进行类型转换    ---jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(jsonStr -> JSONObject.parseObject(jsonStr));
        //TODO 2.过滤数据，保留 page_id 为 home、course_list或course_detail 的数据     ---filter
        SingleOutputStreamOperator<JSONObject> filterObjDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        if ("home".equals(pageId) || "course_list".equals(pageId) || "course_detail".equals(pageId)) {
                            return true;
                        }
                        return false;
                    }
                }
        );
        //filterObjDS.print();
        //TODO 2.设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );

        //TODO 3.按照 mid 分组
        KeyedStream<JSONObject, String> keyByObjDS
                = withWatermarkDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //TODO 4.统计各页面独立访客数       ---flink状态编程,jsonObj->统计的实体类对象
        SingleOutputStreamOperator<DwsTrafficPageViewWindowBean> beanDS = keyByObjDS.process(
                new KeyedProcessFunction<String, JSONObject, DwsTrafficPageViewWindowBean>() {
                    private ValueState<String> homeLastVisitDateState;
                    private ValueState<String> courseListLastVisitDateState;

                    private ValueState<String> courseDetailLastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //todo 详情状态
                        ValueStateDescriptor<String> homevalueStateDescriptor
                                = new ValueStateDescriptor<String>("homevalueStateDescriptor", String.class);

                        //设置状态类型为创建和写，状态失效时间是1天
                        homevalueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1))
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .build()
                        );
                        homeLastVisitDateState = getRuntimeContext().getState(homevalueStateDescriptor);
                        //todo 课程列表状态
                        ValueStateDescriptor<String> courseListvalueStateDescriptor
                                = new ValueStateDescriptor<String>("courseListStateDescriptor", String.class);

                        //设置状态类型为创建和写，状态失效时间是1天
                        courseListvalueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1))
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .build()
                        );
                        courseListLastVisitDateState = getRuntimeContext().getState(courseListvalueStateDescriptor);

                        //todo 课程详情
                        ValueStateDescriptor<String> courseDetailStateDescriptor
                                = new ValueStateDescriptor<String>("courseDetailStateDescriptor", String.class);
                        //设置状态类型为创建和写，状态失效时间是1天
                        courseDetailStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1))
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .build()
                        );
                        courseDetailLastVisitDateState = getRuntimeContext().getState(courseDetailStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, DwsTrafficPageViewWindowBean>.Context context, Collector<DwsTrafficPageViewWindowBean> out) throws Exception {
                        //获取页面id
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        //获取当前日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDateTime(ts);

                        Long homeUvCt = 0L;
                        Long courseListUvCt = 0L;
                        Long courseDetailUvCt = 0L;
                        if ("home".equals(pageId)) {
                            //首页
                            //从状态中获取当前设备上次访问首页的日期
                            String homeLastVisitDate = homeLastVisitDateState.value();
                            if (StringUtils.isEmpty(homeLastVisitDate) || DateFormatUtil.dateTimeToTs(curVisitDate) > DateFormatUtil.dateTimeToTs(homeLastVisitDate)) {
                                homeUvCt = 1L;
                                homeLastVisitDateState.update(curVisitDate);
                            }
                        } else {
                            if ("course_list".equals(pageId)) {
                                //详情页
                                //从状态中获取当前设备上次访问详情页的日期
                                String courseListLastVisitDate = courseListLastVisitDateState.value();
                                if (StringUtils.isEmpty(courseListLastVisitDate) || DateFormatUtil.dateTimeToTs(curVisitDate) > DateFormatUtil.dateTimeToTs(courseListLastVisitDate)) {
                                    courseListUvCt = 1L;
                                    courseListLastVisitDateState.update(curVisitDate);

                                }
                            }else {
                                    String courseDetailLastVisitDate = courseDetailLastVisitDateState.value();
                                    if (StringUtils.isEmpty(courseDetailLastVisitDate) || DateFormatUtil.dateTimeToTs(curVisitDate) > DateFormatUtil.dateTimeToTs(courseDetailLastVisitDate)) {
                                        courseDetailUvCt = 1L;
                                        courseDetailLastVisitDateState.update(curVisitDate);
                                    }
                                }
                            }

                        if (homeUvCt != 0L || courseListUvCt != 0L || courseDetailUvCt != 0) {
                            out.collect(new DwsTrafficPageViewWindowBean(
                                    "",
                                    "",
                                    "",
                                    //jsonObj.getJSONObject("common").getString("mid"),
                                    //jsonObj.getJSONObject("page").getString("page_id"),
                                    homeUvCt,
                                    courseListUvCt,
                                    courseDetailUvCt,
                                    ts
                            ));
                        }
                    }
                }
        );
        //beanDS.print();
        //TODO 5.开窗
        AllWindowedStream<DwsTrafficPageViewWindowBean, TimeWindow> windowDS
                = beanDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 6.聚合
        SingleOutputStreamOperator<DwsTrafficPageViewWindowBean> reduceDS = windowDS.reduce(
                new ReduceFunction<DwsTrafficPageViewWindowBean>() {
                    @Override
                    public DwsTrafficPageViewWindowBean reduce(DwsTrafficPageViewWindowBean v1, DwsTrafficPageViewWindowBean v2) throws Exception {
                        v1.setDetailUvCount(v1.getDetailUvCount() + v2.getDetailUvCount());
                        v1.setListUvCount(v1.getListUvCount() + v2.getListUvCount());
                        v1.setHomeUvCount(v1.getHomeUvCount() + v2.getHomeUvCount());
                        return v1;
                    }
                },
                new AllWindowFunction<DwsTrafficPageViewWindowBean, DwsTrafficPageViewWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<DwsTrafficPageViewWindowBean> input, Collector<DwsTrafficPageViewWindowBean> out) throws Exception {
                        DwsTrafficPageViewWindowBean viewBean = input.iterator().next();
                        String windowStart = DateFormatUtil.tsToDateTime(window.getStart());
                        String windowEnd = DateFormatUtil.tsToDateTime(window.getEnd());
                        String currentDate = DateFormatUtil.tsToDate(window.getStart());
                        viewBean.setStt(windowStart);
                        viewBean.setEdt(windowEnd);
                        viewBean.setCurrentDate(currentDate);
                        out.collect(viewBean);
                    }
                }
        );
        reduceDS.print();  //数据聚合后好奇怪

        //TODO 7.将数据写出到 doris  写不进去？
        reduceDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_course_detail_page_view_window"));

    }
}
