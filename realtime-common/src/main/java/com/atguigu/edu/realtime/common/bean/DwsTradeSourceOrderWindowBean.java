package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: DwsTradeSourceOrderWindowBean
 * Create on: 2024/12/16 17:17
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  交易域来源粒度下单各窗口汇总表
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class DwsTradeSourceOrderWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 来源 ID
    String sourceId;

    // 来源名称
    String sourceName;

    // 订单 ID
    String orderId;

    // 交易总额
    Double orderTotalAmount;

    // 下单独立用户数
    Long orderUuCount;

    // 订单数
    Long orderCount;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
