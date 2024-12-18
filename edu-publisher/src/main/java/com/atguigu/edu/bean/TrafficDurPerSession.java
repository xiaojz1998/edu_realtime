package com.atguigu.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: TrafficDurPerSession
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficDurPerSession {

    // 来源
    String source_name;

    // 各会话页面停留时长
    Double total_during_sec;
}