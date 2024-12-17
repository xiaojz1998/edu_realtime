package com.atguigu.edu.service;

import com.atguigu.edu.bean.ExaminationCourseStats;
import com.atguigu.edu.bean.ExaminationPaperStats;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Title: ExaminationStatsService
 * Create on: 2024/12/17 20:04
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */
public interface ExaminationStatsService {
    List<ExaminationPaperStats> getExaminationPaperStats(Integer date);

    List<ExaminationCourseStats> getExaminationCourseStats(Integer date);
}
