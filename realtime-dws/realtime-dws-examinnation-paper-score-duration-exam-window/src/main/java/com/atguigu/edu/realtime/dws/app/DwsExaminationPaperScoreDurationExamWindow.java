package com.atguigu.edu.realtime.dws.app;

import com.atguigu.edu.realtime.common.base.BaseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Title: DwsExaminationPaperScoreDurationExamWindow
 * Create on: 2024/12/16 4:52
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  考试域试卷分数段粒度考试各窗口汇总表
 *  需要的进程：
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

    }
}
