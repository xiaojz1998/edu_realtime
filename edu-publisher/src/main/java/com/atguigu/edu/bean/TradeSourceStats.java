package com.atguigu.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

/**
 * Title: TradeSourceStats
 * Create on: 2024/12/17 17:36
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */
@Data
@AllArgsConstructor
public class TradeSourceStats {
    // 来源名称
    String SourceName;

    // 订单总额
    BigDecimal orderTotalAmount;

    // 下单独立用户数
    Long orderUuCt;

    // 订单数
    Long orderCt;
}
