package com.atguigu.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Title: ExaminationCourseStats
 * Create on: 2024/12/17 17:32
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */
@Data
@AllArgsConstructor
public class ExaminationCourseStats {
    // 课程名称
    String courseName;

    // 考试人次
    Long examTakenCount;

    // 试卷平均分
    Double avgScore;

    // 平均时长
    Double avgSec;
}
