package com.atguigu.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: TrafficUvCt
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *    各来源流量统计：
 *           粒度：来源
 *           指标：独立访客数，会话总数，会话平均浏览页面数，会话平均停留时长
 *           本节将为上述五个指标各生成一张柱状图
 *
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficUvCt {
    // 来源
    String source_name;

    // 独立访客数
    Long uv_count;

}
