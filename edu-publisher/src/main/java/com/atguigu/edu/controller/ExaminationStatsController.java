package com.atguigu.edu.controller;

import com.atguigu.edu.bean.ExaminationCourseStats;
import com.atguigu.edu.bean.ExaminationPaperStats;
import com.atguigu.edu.service.ExaminationStatsService;
import com.atguigu.edu.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Title: ExaminationStatsController
 * Create on: 2024/12/17 20:49
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */
@RestController
@RequestMapping("/edu/realtime/examination")
public class ExaminationStatsController {

    @Autowired
    ExaminationStatsService examinationStatsService;

    @RequestMapping("/paperStats")
    public String getExaminationPaperCt(@RequestParam(value = "date", defaultValue = "0") Integer date){
        if(date == 0) date = DateFormatUtil.now();
        System.out.println("date = " + date);
        List<ExaminationPaperStats> examinationPaperStatsList = examinationStatsService.getExaminationPaperStats(date);

        //return examinationPaperStatsList.toString();
        if (examinationPaperStatsList == null || examinationPaperStatsList.size() == 0) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < examinationPaperStatsList.size(); i++) {
            ExaminationPaperStats examinationPaperStats = examinationPaperStatsList.get(i);
            String paperTitle = examinationPaperStats.getPaperTitle();
            Long examTakenCount = examinationPaperStats.getExamTakenCount();
            Double avgScore = examinationPaperStats.getAvgScore();
            Double avgSec = examinationPaperStats.getAvgSec();


            rows.append("{\n" +
                    "\t\"paperTitle\": \"" + paperTitle + "\",\n" +
                    "\t\"examTakenCount\": \"" + examTakenCount + "\",\n" +
                    "\t\"avgScore\": \"" + avgScore + "\",\n" +
                    "\t\"avgSec\": \"" + avgSec + "\"\n" +
                    "}");
            if (i < examinationPaperStatsList.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }
        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"columns\": [\n" +
                "      {\n" +
                "        \"name\": \"试卷名称\",\n" +
                "        \"id\": \"paperName\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"考试人次\",\n" +
                "        \"id\": \"examTakenCount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"平均分\",\n" +
                "        \"id\": \"avgScore\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"平均用时\",\n" +
                "        \"id\": \"avgSec\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }

    @RequestMapping("/courseStats")
    public String getExaminationCourseCt(@RequestParam(value = "date", defaultValue = "0") Integer date){
        if(date == 0) date = DateFormatUtil.now();
        List<ExaminationCourseStats> examinationCourseStatsList = examinationStatsService.getExaminationCourseStats(date);
        if (examinationCourseStatsList == null || examinationCourseStatsList.size() == 0) {
            return "";
        }
        StringBuilder rows = new StringBuilder("[");
        for (int i = 0; i < examinationCourseStatsList.size(); i++) {
            ExaminationCourseStats examinationCourseStats = examinationCourseStatsList.get(i);
            String courseName = examinationCourseStats.getCourseName();
            Long examTakenCount = examinationCourseStats.getExamTakenCount();
            Double avgScore = examinationCourseStats.getAvgScore();
            Double avgSec = examinationCourseStats.getAvgSec();


            rows.append("{\n" +
                    "\t\"courseName\": \"" + courseName + "\",\n" +
                    "\t\"examTakenCount\": \"" + examTakenCount + "\",\n" +
                    "\t\"avgScore\": \"" + avgScore + "\",\n" +
                    "\t\"avgSec\": \"" + avgSec + "\"\n" +
                    "}");
            if (i < examinationCourseStatsList.size() - 1) {
                rows.append(",");
            } else {
                rows.append("]");
            }
        }
        return "{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"columns\": [\n" +
                "      {\n" +
                "        \"name\": \"课程名称\",\n" +
                "        \"id\": \"courseName\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"考试人次\",\n" +
                "        \"id\": \"examTakenCount\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"平均分\",\n" +
                "        \"id\": \"avgScore\"\n" +
                "      },\n" +
                "      {\n" +
                "        \"name\": \"平均用时\",\n" +
                "        \"id\": \"avgSec\"\n" +
                "      }\n" +
                "    ],\n" +
                "    \"rows\": " + rows + "\n" +
                "  }\n" +
                "}";
    }
}
