package com.atguigu.edu.realtime.common.bean;

/**
 * Title: DwsTrafficPageViewWindowBean
 * Create on: 2024/12/16
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *    流量域页面浏览各窗口汇总表的实体类：DwsTrafficPageViewWindowBean
 */

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTrafficPageViewWindowBean {

    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    //当前时间
    String currentDate;

   /* // 设备 ID
    String mid;

    // 页面 ID
    String pageId;*/

    // 首页独立访客数
    Long homeUvCount;

    // 课程列表页独立访客数
    Long listUvCount;

    // 课程详情页独立访客数
    Long detailUvCount;

    // 时间戳
    @JSONField(serialize = false)
    Long ts;
}
