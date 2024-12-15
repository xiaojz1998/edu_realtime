package com.atguigu.edu.realtime.common.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * Title: DimJoinFunction
 * Create on: 2024/12/16 4:01
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  维度关联需要实现的接口
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj);

    String getTableName();

    String getRowKey(T obj);
}
