package com.atguigu.edu.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: DwdTableProcess
 * Create on: 2024/12/16 0:17
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DwdTableProcess {
    // 来源表
    String sourceTable;

    // 操作类型
    String sourceType;

    // 输出表
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 配置表操作类型
    String op;
}
