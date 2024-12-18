package com.atguigu.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: TrafficKeywords
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficKeywords {

    // 关键词
    String keyword;

    // 关键词评分
    Integer keywordCount;
}