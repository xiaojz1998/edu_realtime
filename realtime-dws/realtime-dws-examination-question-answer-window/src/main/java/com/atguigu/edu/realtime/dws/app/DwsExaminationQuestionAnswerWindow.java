package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.bean.DwsExaminationQuestionAnswerWindowBean;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.realtime.common.function.DimAsyncFunction;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Title: DwsExaminationQuestionAnswerWindow
 * Create on: 2024/12/16 5:26
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  考试域题目粒度答题各窗口汇总表
 *  需要的进程：
 *   zk、kafka、maxwell、DwdBaseDb
 *
 */
public class DwsExaminationQuestionAnswerWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsExaminationQuestionAnswerWindow().start(
                10036,
                4,
                "dws_examination_question_answer_window",
                Constant.TOPIC_DWD_EXAMINATION_TEST_QUESTION
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        
        // 转化数据结构
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> mapDS = kafkaStrDS.map(
                new MapFunction<String, DwsExaminationQuestionAnswerWindowBean>() {
                    @Override
                    public DwsExaminationQuestionAnswerWindowBean map(String s) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(s);
                        String questionId = jsonObj.getString("question_id");
                        String isCorrect = jsonObj.getString("is_correct");
                        Long ts = jsonObj.getLong("ts") * 1000L;
                        Long correctAnswerCount = isCorrect.equals("1") ? 1L : 0L;
                        return DwsExaminationQuestionAnswerWindowBean.builder()
                                .question_id(questionId)
                                .correctAnswerCount(correctAnswerCount)
                                .answer_count(1L)
                                .ts(ts)
                                .build();
                    }
                }
        );

        // 设置水位线
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> withWatermarkDS = mapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<DwsExaminationQuestionAnswerWindowBean>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<DwsExaminationQuestionAnswerWindowBean>() {
                                    @Override
                                    public long extractTimestamp(DwsExaminationQuestionAnswerWindowBean dwsExaminationQuestionAnswerWindowBean, long l) {
                                        return dwsExaminationQuestionAnswerWindowBean.getTs();
                                    }
                                }
                        )
        );

        // 分组 开窗 聚合
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> reduceDS = withWatermarkDS.keyBy(DwsExaminationQuestionAnswerWindowBean::getQuestion_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<DwsExaminationQuestionAnswerWindowBean>() {
                            @Override
                            public DwsExaminationQuestionAnswerWindowBean reduce(DwsExaminationQuestionAnswerWindowBean dwsExaminationQuestionAnswerWindowBean, DwsExaminationQuestionAnswerWindowBean t1) throws Exception {
                                dwsExaminationQuestionAnswerWindowBean.setAnswer_count(dwsExaminationQuestionAnswerWindowBean.getAnswer_count() + t1.getAnswer_count());
                                dwsExaminationQuestionAnswerWindowBean.setCorrectAnswerCount(dwsExaminationQuestionAnswerWindowBean.getCorrectAnswerCount() + t1.getCorrectAnswerCount());
                                return dwsExaminationQuestionAnswerWindowBean;
                            }
                        },
                        new ProcessWindowFunction<DwsExaminationQuestionAnswerWindowBean, DwsExaminationQuestionAnswerWindowBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<DwsExaminationQuestionAnswerWindowBean, DwsExaminationQuestionAnswerWindowBean, String, TimeWindow>.Context context, Iterable<DwsExaminationQuestionAnswerWindowBean> iterable, Collector<DwsExaminationQuestionAnswerWindowBean> collector) throws Exception {
                                String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                String curDate = DateFormatUtil.tsToDate(context.window().getStart());
                                DwsExaminationQuestionAnswerWindowBean next = iterable.iterator().next();
                                next.setStt(start);
                                next.setEdt(end);
                                next.setCurDate(curDate);
                                collector.collect(next);
                            }
                        }
                );

        // 补充题目内容
        SingleOutputStreamOperator<DwsExaminationQuestionAnswerWindowBean> withQuestionTxtDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<DwsExaminationQuestionAnswerWindowBean>() {
                    @Override
                    public void addDims(DwsExaminationQuestionAnswerWindowBean obj, JSONObject dimJsonObj) {
                        obj.setQuestion_txt(dimJsonObj.getString("question_txt"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_test_question_info";
                    }

                    @Override
                    public String getRowKey(DwsExaminationQuestionAnswerWindowBean obj) {
                        return obj.getQuestion_id();
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        withQuestionTxtDS.print("withQuestionTxtDS");
        withQuestionTxtDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_examination_question_answer_window"));
    }
}
