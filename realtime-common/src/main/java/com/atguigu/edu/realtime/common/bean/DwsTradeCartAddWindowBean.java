package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: DwsTradeCartAddWindowBean
 * Create on: 2024/12/16 17:07
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  交易域加购各窗口汇总表
 *
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTradeCartAddWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 加购独立用户数
    Long cartAddUvCount;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
