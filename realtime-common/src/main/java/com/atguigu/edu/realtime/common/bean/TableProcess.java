package com.atguigu.edu.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: TableProcess
 * Create on: 2024/12/14 11:06
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {
    // 来源表名
    String sourceTable;

    // 目标表名
    String sinkTable;

    // 输出字段
    String sinkColumns;

    // 数据到 hbase 的列族 没有，需要补充
    String sinkFamily;

    // sink到 hbase 的时候的主键字段
    String sinkPk;

    // 配置表操作类型  没有 需要补充
    String op;
}
