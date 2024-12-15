package com.atguigu.edu.realtime.dws.app;

import com.atguigu.edu.realtime.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Title: DwsExaminationQuestionAnswerWindow
 * Create on: 2024/12/16 5:26
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  考试域题目粒度答题各窗口汇总表
 *  需要的进程：
 *
 */
public class DwsExaminationQuestionAnswerWindow extends BaseApp {
    public static void main(String[] args) {

    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

    }
}
