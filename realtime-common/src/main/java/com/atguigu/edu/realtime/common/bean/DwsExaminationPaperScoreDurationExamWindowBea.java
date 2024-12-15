package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: DwsExaminationPaperScoreDurationExamWindowBea
 * Create on: 2024/12/16 5:20
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 * 考试域试卷分数段粒度考试各窗口汇总表
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsExaminationPaperScoreDurationExamWindowBea {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;


    // 试卷 ID
    String paper_id;

    // 试卷名称
    String paper_title;

    // 分数段
    String score_duration;

    // 用户数
    Long user_count;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
