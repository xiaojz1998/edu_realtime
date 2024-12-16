package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: DwsTradeCourseOrderWindowBean
 * Create on: 2024/12/16 17:14
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *   交易域课程粒度下单各窗口汇总表
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTradeCourseOrderWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 课程 ID
    String courseId;

    // 课程名称
    String courseName;

    // 科目 ID
    String subjectId;

    // 科目名称
    String subjectName;

    // 类别 ID
    String categoryId;

    // 类别名称
    String categoryName;

    // 下单总金额
    Double orderTotalAmount;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
