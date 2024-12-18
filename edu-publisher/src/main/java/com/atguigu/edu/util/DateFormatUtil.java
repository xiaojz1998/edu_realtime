package com.atguigu.edu.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * Title: DateFormatUtil
 * Create on: 2024/12/17
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 */
public class DateFormatUtil {
    //获取当天日期的整数形式
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
