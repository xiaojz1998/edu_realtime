package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: DwsTradeProvinceOrderWindowBean
 * Create on: 2024/12/16 17:20
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *      交易域省份粒度下单各窗口汇总表
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTradeProvinceOrderWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 省份 ID
    String provinceId;

    // 省份名称
    String provinceName;

    // 用户 ID
    String userId;

    // 订单总额
    Double orderTotalAmount;

    // 下单独立用户数
    Long orderUuCount;

    // 订单数
    Long orderCount;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
