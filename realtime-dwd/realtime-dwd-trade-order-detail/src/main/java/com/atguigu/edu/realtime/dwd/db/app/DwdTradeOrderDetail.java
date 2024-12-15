package com.atguigu.edu.realtime.dwd.db.app;

import com.atguigu.edu.realtime.common.base.BaseSQLAPP;
import com.atguigu.edu.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Title: DwdTradeOrderDetail
 * Create on: 2024/12/15 17:31
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *   筛选订单明细表和订单表数据，
 *   读取 dwd_traffic_page_log 主题数据，
 *   筛选订单页日志，关联三张表获得交易域下单事务事实表，写入 Kafka 对应主题。
 *   下单事实表
 */
public class DwdTradeOrderDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(
                10014,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        // TODO：设置表状态 ttl
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        // TODO： kafka的topic_db主题中读取数据
        // readOdsDb读取topic_db 的kafka source
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        // TODO 4. 从 Kafka 读取日志数据，封装为 Flink SQL 表



        // TODO：筛选订单明细表
        Table orderDetail = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['order_id'] order_id,\n" +
                "data['user_id'] user_id,\n" +
                "data['course_id'] course_id,\n" +
                "date_format(data['create_time'], 'yyyy-MM-dd') date_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['origin_amount'] origin_amount,\n" +
                "data['coupon_reduce'] coupon_reduce,\n" +
                "data['final_amount'] final_amount,\n" +
                "ts\n" +
                "from `topic_db` where `table` = 'order_detail' " +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_detail", orderDetail);
        // TODO: 筛选出订单表
        Table orderInfo = tableEnv.sqlQuery("select \n" +
                "data['id'] id,\n" +
                "data['province_id'] province_id,\n" +
                "data['out_trade_no'] out_trade_no,\n" +
                "data['session_id'] session_id,\n" +
                "data['trade_body'] trade_body\n" +
                "from `topic_db`\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'insert'");
        tableEnv.createTemporaryView("order_info", orderInfo);


    }
}
