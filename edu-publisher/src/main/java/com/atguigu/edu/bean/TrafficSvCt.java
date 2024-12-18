package com.atguigu.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: TrafficSvCt
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficSvCt {

    // 来源
    String source_name;

    // 会话数
    Long total_session_count;
}
