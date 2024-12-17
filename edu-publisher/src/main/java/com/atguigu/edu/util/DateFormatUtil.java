package com.atguigu.edu.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * @author xiaojz
 * @date 2024/12/17
 */
public class DateFormatUtil {
    //获取当天日期的整数形式
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
