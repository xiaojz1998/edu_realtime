package com.atguigu.edu.bean;

/**
 * Title: TrafficVisitorStatsPerHour
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficVisitorStatsPerHour {

    // 小时
    Integer hr;

    // 独立访客数
    Long uv_count;

    // 页面浏览数
    Long page_view_count;

    // 新访客数
    Long newUvCt;
}
