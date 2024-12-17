package com.atguigu.edu.service.impl;

import com.atguigu.edu.bean.ExaminationCourseStats;
import com.atguigu.edu.bean.ExaminationPaperStats;
import com.atguigu.edu.mapper.ExaminationStatsMapper;
import com.atguigu.edu.service.ExaminationStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Title: ExaminationStatsServiceImpl
 * Create on: 2024/12/17 20:05
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */
@Service
public class ExaminationStatsServiceImpl implements ExaminationStatsService {
    @Autowired
    ExaminationStatsMapper examinationStatsMapper;
    @Override
    public List<ExaminationPaperStats> getExaminationPaperStats(Integer date) {
        return examinationStatsMapper.selectExaminationPaperStats(date);
    }

    @Override
    public List<ExaminationCourseStats> getExaminationCourseStats(Integer date) {
        return examinationStatsMapper.selectExaminationCourseStats(date);
    }
}
