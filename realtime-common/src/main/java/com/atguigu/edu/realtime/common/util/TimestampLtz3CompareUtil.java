package com.atguigu.edu.realtime.common.util;

/**
 * Title: TimestampLtz3CompareUtil
 * Create on: 2024/12/17 1:51
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 */
public class TimestampLtz3CompareUtil {
    public static int compare(String timestamp1, String timestamp2) {
        // 数据格式 2022-04-01 10:20:47.302Z
        // 1. 去除末尾的时区标志，'Z' 表示 0 时区
        String cleanedTime1 = timestamp1.substring(0, timestamp1.length() - 1);
        String cleanedTime2 = timestamp2.substring(0, timestamp2.length() - 1);
        // 2. 比较时间
        return cleanedTime1.compareTo(cleanedTime2);
    }

    public static void main(String[] args) {
        System.out.println(compare("2022-04-01 11:10:55.04Z",
                "2022-04-01 11:10:55.039Z"));
    }
}
