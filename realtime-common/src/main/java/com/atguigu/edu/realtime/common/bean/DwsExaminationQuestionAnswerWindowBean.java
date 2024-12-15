package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: DwsExaminationQuestionAnswerWindowBean
 * Create on: 2024/12/16 5:23
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  考试域题目粒度答题各窗口汇总表
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsExaminationQuestionAnswerWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;
    // 当天日期
    String curDate;


    // 题目 ID
    String question_id;

    // 题目内容
    String question_txt;

    // 正确答题次数
    Long correctAnswerCount;

    // 答题次数
    Long answer_count;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
