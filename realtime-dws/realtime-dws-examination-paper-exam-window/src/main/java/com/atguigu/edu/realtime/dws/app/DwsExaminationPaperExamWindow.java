package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsExaminationPaperExamWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.DimAsyncFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Title: DwsExaminationPaperExamWindow
 * Create on: 2024/12/16 2:26
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  考试域试卷粒度考试各窗口汇总表
 *  需要启动的进程：
 *      zk、kafka、maxwell、DwdBaseDb
 */
public class DwsExaminationPaperExamWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsExaminationPaperExamWindow().start(
                10034,
                4,
                "dws_examination_paper_exam_window",
                Constant.TOPIC_DWD_EXAMINATION_TEST_PAPER
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // TODO 转换结构
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> mapDS = kafkaStrDS.map(
                new MapFunction<String, DwsExaminationPaperExamWindowBean>() {
                    @Override
                    public DwsExaminationPaperExamWindowBean map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        long ts = jsonObject.getLong("ts") * 1000;
                        Double score = jsonObject.getDouble("score");
                        return DwsExaminationPaperExamWindowBean.builder()
                                .paperId(jsonObject.getString("paper_id"))
                                .examTakenCount(1L)
                                .examTotalScore((long) score.doubleValue())
                                .examTotalDuringSec(jsonObject.getLong("duration_sec"))
                                .ts(ts)
                                .build();
                    }
                }
        );
        mapDS.print();
        // TODO 添加水位线
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> withWatermarkDS = mapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsExaminationPaperExamWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(5l))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsExaminationPaperExamWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsExaminationPaperExamWindowBean dwsExaminationPaperExamWindowBean, long l) {
                                        return dwsExaminationPaperExamWindowBean.getTs();
                                    }
                                }
                        )
        );
        // TODO 分组 开窗 聚合
        KeyedStream<DwsExaminationPaperExamWindowBean, String> withKeyedDS = withWatermarkDS.keyBy(DwsExaminationPaperExamWindowBean::getPaperId);
        WindowedStream<DwsExaminationPaperExamWindowBean, String, TimeWindow> withWindowDS = withKeyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> reduceDS = withWindowDS.reduce(
                new ReduceFunction<DwsExaminationPaperExamWindowBean>() {
                    @Override
                    public DwsExaminationPaperExamWindowBean reduce(DwsExaminationPaperExamWindowBean value1, DwsExaminationPaperExamWindowBean value2) throws Exception {
                        value1.setExamTakenCount(value1.getExamTakenCount()+value2.getExamTakenCount());
                        value1.setExamTotalDuringSec(value1.getExamTotalDuringSec()+value2.getExamTotalDuringSec());
                        value1.setExamTotalScore(value1.getExamTotalScore()+value2.getExamTotalScore());
                        return value1;
                    }
                },
                new ProcessWindowFunction<DwsExaminationPaperExamWindowBean, DwsExaminationPaperExamWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<DwsExaminationPaperExamWindowBean, DwsExaminationPaperExamWindowBean, String, TimeWindow>.Context context, Iterable<DwsExaminationPaperExamWindowBean> iterable, Collector<DwsExaminationPaperExamWindowBean> collector) throws Exception {
                        String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String curDate = DateFormatUtil.tsToDate(context.window().getStart());
                        DwsExaminationPaperExamWindowBean next = iterable.iterator().next();
                        next.setStt(start);
                        next.setEdt(end);
                        next.setCurDate(curDate);
                        collector.collect(next);
                    }
                }
        );
        reduceDS.print("reduceDS");
        // TODO 维度关联补充维度信息
        //  关联试卷表
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> withPaperDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<DwsExaminationPaperExamWindowBean>() {
                    @Override
                    public void addDims(DwsExaminationPaperExamWindowBean obj, JSONObject dimJsonObj) {
                        obj.setPaperTitle(dimJsonObj.getString("paper_title"));
                        obj.setCourseId(dimJsonObj.getString("course_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_test_paper";
                    }

                    @Override
                    public String getRowKey(DwsExaminationPaperExamWindowBean obj) {
                        return obj.getPaperId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //  关联课程表
        SingleOutputStreamOperator<DwsExaminationPaperExamWindowBean> withCourseDS = AsyncDataStream.unorderedWait(
                withPaperDS,
                new DimAsyncFunction<DwsExaminationPaperExamWindowBean>() {
                    @Override
                    public void addDims(DwsExaminationPaperExamWindowBean obj, JSONObject dimJsonObj) {
                        obj.setCourseName(dimJsonObj.getString("course_name"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_course_info";
                    }

                    @Override
                    public String getRowKey(DwsExaminationPaperExamWindowBean obj) {
                        return obj.getCourseId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withCourseDS.print("withCourseDS");
        // Sink 到doris
    }
}
