package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsExaminationPaperScoreDurationExamWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.function.DimAsyncFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
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
 * Title: DwsExaminationPaperScoreDurationExamWindow
 * Create on: 2024/12/16 4:52
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  考试域试卷分数段粒度考试各窗口汇总表
 *  需要的进程：
 *      zk、kafka、maxwell、DwdBaseDb
 *
 */
public class DwsExaminationPaperScoreDurationExamWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsExaminationPaperScoreDurationExamWindow().start(
                10035,
                4,
                "dws_examination_paper_score_duration_exam_window",
                Constant.TOPIC_DWD_EXAMINATION_TEST_PAPER
        );

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 转换结构
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> mapDS = kafkaStrDS.map(
                new MapFunction<String, DwsExaminationPaperScoreDurationExamWindowBean>() {
                    @Override
                    public DwsExaminationPaperScoreDurationExamWindowBean map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        Double score = jsonObject.getDouble("score");
                        long ts = jsonObject.getLong("ts") * 1000;
                        String scoreDuration;
                        if (score < 60) {
                            scoreDuration = "[0,60)";
                        } else if (score < 70) {
                            scoreDuration = "[60,70)";
                        } else if (score < 80) {
                            scoreDuration = "[70,80)";
                        } else if (score < 90) {
                            scoreDuration = "[80,90)";
                        } else if (score <= 100) {
                            scoreDuration = "[90,100]";
                        } else {
                            scoreDuration = "";
                        }
                        return DwsExaminationPaperScoreDurationExamWindowBean.builder()
                                .paper_id(jsonObject.getString("paper_id"))
                                .score_duration(scoreDuration)
                                .user_count(1L)
                                .ts(ts)
                                .build();
                    }
                }
        );
        // 添加水位线
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> withWatermarkDS = mapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsExaminationPaperScoreDurationExamWindowBean>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsExaminationPaperScoreDurationExamWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsExaminationPaperScoreDurationExamWindowBean dwsExaminationPaperScoreDurationExamWindowBean, long l) {
                                        return dwsExaminationPaperScoreDurationExamWindowBean.getTs();
                                    }
                                }
                        )
        );
        // 分组
        KeyedStream<DwsExaminationPaperScoreDurationExamWindowBean, String> keyedDS = withWatermarkDS.keyBy(
                new KeySelector<DwsExaminationPaperScoreDurationExamWindowBean, String>() {
                    @Override
                    public String getKey(DwsExaminationPaperScoreDurationExamWindowBean dwsExaminationPaperScoreDurationExamWindowBean) throws Exception {
                        return dwsExaminationPaperScoreDurationExamWindowBean.getPaper_id() + dwsExaminationPaperScoreDurationExamWindowBean.getScore_duration();
                    }
                }
        );

        // 开窗聚合
        WindowedStream<DwsExaminationPaperScoreDurationExamWindowBean, String, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> reduceDS = windowDS.reduce(
                new ReduceFunction<DwsExaminationPaperScoreDurationExamWindowBean>() {
                    @Override
                    public DwsExaminationPaperScoreDurationExamWindowBean reduce(DwsExaminationPaperScoreDurationExamWindowBean dwsExaminationPaperScoreDurationExamWindowBean, DwsExaminationPaperScoreDurationExamWindowBean t1) throws Exception {
                        dwsExaminationPaperScoreDurationExamWindowBean.setUser_count(dwsExaminationPaperScoreDurationExamWindowBean.getUser_count() + t1.getUser_count());
                        return dwsExaminationPaperScoreDurationExamWindowBean;
                    }
                },
                new ProcessWindowFunction<DwsExaminationPaperScoreDurationExamWindowBean, DwsExaminationPaperScoreDurationExamWindowBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<DwsExaminationPaperScoreDurationExamWindowBean, DwsExaminationPaperScoreDurationExamWindowBean, String, TimeWindow>.Context context, Iterable<DwsExaminationPaperScoreDurationExamWindowBean> iterable, Collector<DwsExaminationPaperScoreDurationExamWindowBean> collector) throws Exception {
                        DwsExaminationPaperScoreDurationExamWindowBean next = iterable.iterator().next();
                        String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                        String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                        String curDate = DateFormatUtil.tsToDate(context.window().getStart());
                        next.setStt(start);
                        next.setEdt(end);
                        next.setCurDate(curDate);
                        collector.collect(next);
                    }
                }
        );
        // 维度关联试卷名称
        SingleOutputStreamOperator<DwsExaminationPaperScoreDurationExamWindowBean> withPaperTitleDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<DwsExaminationPaperScoreDurationExamWindowBean>() {
                    @Override
                    public void addDims(DwsExaminationPaperScoreDurationExamWindowBean obj, JSONObject dimJsonObj) {
                        obj.setPaper_title(dimJsonObj.getString("paper_title"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_test_paper";
                    }

                    @Override
                    public String getRowKey(DwsExaminationPaperScoreDurationExamWindowBean obj) {
                        return obj.getPaper_id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withPaperTitleDS.print("withPaperTitleDS");
        withPaperTitleDS
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_examination_paper_score_duration_exam_window"));
    }
}
