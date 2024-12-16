package com.atguigu.edu.realtime.common.bean;

/**
 * Title: KeywordBean
 * Create on: 2024/12/16
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *   关键词实体类
 */
public class KeywordBean {
    // 窗口起始时间
    private String stt;

    // 窗口闭合时间
    private String edt;

    // 关键词来源
    private String source;

    // 关键词
    private String keyword;

    // 关键词出现频次
    private Long keyword_count;

    // 时间戳
    private Long ts;
}
