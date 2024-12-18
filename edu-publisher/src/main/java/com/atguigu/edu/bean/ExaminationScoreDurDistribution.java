package com.atguigu.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Title: ExaminationScoreDurDistribution
 * Create on: 2024/12/17 17:32
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */

@Data
@AllArgsConstructor
public class ExaminationScoreDurDistribution {

    // 分数段
    String scoreDuration;

    // 人数
    Long userCout;
}
