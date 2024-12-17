package com.atguigu.edu.realtime.dwd.db.app;

import com.atguigu.edu.realtime.common.base.BaseSQLAPP;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.time.ZoneId;

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
 *   开启：
 *      zk kafka maxwell flume DwdBaseLog DwdTradeOrderDetail
 *
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
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        // TODO： kafka的topic_db主题中读取数据
        // readOdsDb读取topic_db 的kafka source
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
        // TODO 4. 从 Kafka 读取日志数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table page_log(" +
                "`common` map<String, String>,\n" +
                "`page` map<String, String>,\n" +
                "`ts` String\n" +
                ")" + SQLUtil.getKafkaDDL("dwd_traffic_page",Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        //tableEnv.sqlQuery("select * from page_log").execute().print();


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

        // TODO 筛选订单页面日志，获取 source_id
        Table filteredLog = tableEnv.sqlQuery("select " +
                "common['sid'] session_id,\n" +
                "common['sc'] source_id\n" +
                "from page_log\n" +
                "where page['page_id'] = 'order'");
        tableEnv.createTemporaryView("filter_log", filteredLog);

        // TODO  关联三张表获得订单明细表
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "od.id,\n" +
                "order_id,\n" +
                "user_id,\n" +
                "course_id,\n" +
                "province_id,\n" +
                "date_id,\n" +
                "oi.session_id,\n" +
                "source_id,\n" +
                "create_time,\n" +
                "origin_amount,\n" +
                "coupon_reduce coupon_reduce_amount,\n" +
                "final_amount,\n" +
                "out_trade_no,\n" +
                "trade_body,\n" +
                "ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join filter_log fl\n" +
                "on oi.session_id = fl.session_id");
        tableEnv.createTemporaryView("result_table", resultTable);

        //tableEnv.executeSql("select * from result_table").print();

        // TODO upsert kafka
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "province_id string,\n" +
                "date_id string,\n" +
                "session_id string,\n" +
                "source_id string,\n" +
                "create_time string,\n" +
                "origin_amount string,\n" +
                "coupon_reduce_amount string,\n" +
                "final_amount string,\n" +
                "out_trade_no string,\n" +
                "trade_body string,\n" +
                "ts bigint,\n" +
                "row_op_ts TIMESTAMP_LTZ(3) ,\n" +
                "primary key(id) not enforced\n" +
                ")" + SQLUtil.getUpsertKafkaDDL("dwd_trade_order_detail"));

        resultTable.executeInsert("dwd_trade_order_detail");

    }
}
