package com.atguigu.edu.bean;

import org.springframework.util.DigestUtils;

import java.nio.charset.StandardCharsets;

/**
 * Title: ExaminationQuestionStats
 * Create on: 2024/12/17 17:32
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */

public class ExaminationQuestionStats {
    // 题目
    String questionTxt;

    // 正确次数
    Long correctAnswerCt;

    // 答题次数
    Long answerCt;

    // 正确率
    Double accuracyRate;

    public String processQuestionTxt(){
        String md5 = DigestUtils.md5DigestAsHex(questionTxt.getBytes(StandardCharsets.UTF_8));
        return md5.substring(0, 10);
    }
}
