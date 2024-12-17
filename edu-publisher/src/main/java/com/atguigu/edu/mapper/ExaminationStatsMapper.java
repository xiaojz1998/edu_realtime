package com.atguigu.edu.mapper;

import com.atguigu.edu.bean.ExaminationCourseStats;
import com.atguigu.edu.bean.ExaminationPaperStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * Title: ExaminationStatsMapper
 * Create on: 2024/12/17 19:17
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  考试域统计mapper 接口
 */
@Mapper
public interface ExaminationStatsMapper {

    @Select("SELECT \n" +
            "\tpaper_title,\n" +
            "\tSUM(exam_taken_count) exam_taken_count,\n" +
            "\tSUM(exam_total_score)/ SUM(exam_taken_count) exam_avg_score,\n" +
            "\tSUM(exam_total_during_sec)/ SUM(exam_taken_count) exam_avg_sec\n" +
            "\tFROM edu_realtime.dws_examination_paper_exam_window\n" +
            "\tPARTITION par#{date}\n" +
            "\tGROUP BY paper_id,paper_title\n" +
            "\tORDER BY exam_taken_count DESC\n" +
            "\tLIMIT 10;")
    List<ExaminationPaperStats> selectExaminationPaperStats(@Param("date") Integer date);


    @Select("SELECT \n" +
            "\tcourse_name,\n" +
            "\tSUM(exam_taken_count) exam_taken_count,\n" +
            "\tSUM(exam_total_score)/SUM(exam_taken_count)  exam_avg_score,\n" +
            "\tSUM(exam_total_during_sec)/ SUM(exam_taken_count) exam_avg_sec\n" +
            "\tFROM edu_realtime.dws_examination_paper_exam_window \n" +
            "\tPARTITION par#{date}\n" +
            "\tGROUP BY course_id,course_name\n" +
            "\tORDER BY exam_taken_count DESC\n" +
            "\tLIMIT 10")
    List<ExaminationCourseStats> selectExaminationCourseStats(@Param("date") Integer date);
}
