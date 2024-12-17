package com.atguigu.edu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

/**
 * Title: TradeProvinceOrderAmount
 * Create on: 2024/12/17 17:37
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */
@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;

    // 订单总额
    BigDecimal orderTotalAmount;
}
