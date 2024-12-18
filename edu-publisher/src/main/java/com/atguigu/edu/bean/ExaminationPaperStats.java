package com.atguigu.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Title: ExaminationPaperStats
 * Create on: 2024/12/17 17:08
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */
@Data
@AllArgsConstructor
public class ExaminationPaperStats {
    // 试卷名称
    String paperTitle;

    // 考试人次
    Long examTakenCount;

    // 试卷平均分
    Double avgScore;

    // 平均时长
    Double avgSec;
}
