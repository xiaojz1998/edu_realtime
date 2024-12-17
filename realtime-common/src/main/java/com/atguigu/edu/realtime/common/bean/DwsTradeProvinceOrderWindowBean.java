package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Set;

/**
 * Title: DwsTradeProvinceOrderWindowBean
 * Create on: 2024/12/16
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
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

    // 现在时间
    String curDate;


    // 省份 ID
    String provinceId;

    @Builder.Default
    // 省份名称
    String provinceName = "";

    @Builder.Default
    // 用户 ID
    String userId = "";

    // 累积下单总金额
    BigDecimal orderTotalAmount;

    // 下单独立用户数
    Long orderUuCount;

    // 累计下单次数
    Long orderCount;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
    @JSONField(serialize = false)
    Set<String> orderIdSet;
}
