package com.atguigu.edu.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Title: DwsTrafficForSourcePvBean
 * Create on: 2024/12/16
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *    流量域版本-来源-地区-访客类别粒度页面浏览实体类
 */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public class DwsTrafficForSourcePvBean {
        // 窗口起始时间
        String stt;   //todo 开窗获取

        // 窗口结束时间
        String edt;     //todo 开窗获取

       //今天时间
        String cur_date;     //todo 开窗获取


        // 版本号
        String versionCode;  // todo 日志的common字段vc

        // 来源 ID
        String sourceId;    // todo 日志的common字段sc

        // 来源名称
        String sourceName;  // todo

        // 省份 ID
        String ar;        // todo 日志的common字段ar

        // 新老访客状态标记
        String isNew;      //todo 日志的common字段is_new

        // 省份名称
        String provinceName;   // todo

        // 独立访客数
        Long uvCount;     //todo ---度量值1

        // 会话总数
        Long totalSessionCount;   //todo ---度量值2

        // 页面浏览数
        Long pageViewCount;     //todo ---度量值3

        // 页面总停留时长
        Long totalDuringTime;   //todo ---度量值4

        // 时间戳
        Long ts;    //日志的ts
    }
