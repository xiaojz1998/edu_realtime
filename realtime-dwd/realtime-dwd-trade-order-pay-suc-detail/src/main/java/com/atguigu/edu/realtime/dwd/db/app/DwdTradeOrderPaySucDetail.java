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
 * Title: DwdTradeOrderPaySucDetail
 * Create on: 2024/12/15 21:01
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *   支付成功事实表
 *   需要启动的进程
 *      zk、kafka、maxwell、hdfs、hbase、Dwdlog、DwdTradeOrderDetail、DwdTradeOrderPaySucDetail
 *
 */
public class DwdTradeOrderPaySucDetail extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(
                10016,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

        // 设置状态持续时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15*60 + 5));
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        // 从kafka 读取业务数据
        readOdsDb(tableEnv,Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);

        // TODO  读取 Kafka dwd_trade_order_detail 主题数据，封装为 Flink SQL 表
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
                "row_op_ts TIMESTAMP_LTZ(3) " +
                ")" + SQLUtil.getKafkaDDL("dwd_trade_order_detail",Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS ));

        // TODO 筛选支付成功数据
        Table paymentSuc = tableEnv.sqlQuery("select\n" +
                "data['alipay_trade_no'] alipay_trade_no,\n" +
                "data['trade_body'] trade_body,\n" +
                "data['order_id'] order_id,\n" +
                "data['payment_type'] payment_type,\n" +
                "data['payment_status'] payment_status,\n" +
                "data['callback_time'] callback_time,\n" +
                "data['callback_content'] callback_content,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'payment_info'\n" +
                "and data['payment_status']='1602'");
        tableEnv.createTemporaryView("payment_suc", paymentSuc);
        // TODO 关联两张表
        Table resultTable = tableEnv.sqlQuery("" +
                "select \n" +
                "id,\n" +
                "od.order_id order_id,\n" +
                "user_id,\n" +
                "course_id,\n" +
                "province_id,\n" +
                "date_id,\n" +
                "alipay_trade_no,\n" +
                "pay_suc.trade_body,\n" +
                "payment_type,\n" +
                "payment_status,\n" +
                "callback_time,\n" +
                "callback_content,\n" +
                "origin_amount,\n" +
                "coupon_reduce_amount,\n" +
                "final_amount,\n" +
                "pay_suc.ts ts,\n" +
                "row_op_ts\n" +
                "from payment_suc pay_suc\n" +
                "join dwd_trade_order_detail od\n" +
                "on pay_suc.order_id = od.order_id");
        //tableEnv.createTemporaryView("result_table", resultTable);
        // TODO  创建 Kafka dwd_trade_pay_suc_detail 表
        tableEnv.executeSql("create table dwd_trade_pay_suc_detail(\n" +
                "id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "course_id string,\n" +
                "province_id string,\n" +
                "date_id string,\n" +
                "alipay_trade_no string,\n" +
                "trade_body string,\n" +
                "payment_type string,\n" +
                "payment_status string,\n" +
                "callback_time string,\n" +
                "callback_content string,\n" +
                "original_amount string,\n" +
                "coupon_reduce_amount string,\n" +
                "final_amount string,\n" +
                "ts bigint,\n" +
                "row_op_ts TIMESTAMP_LTZ(3) ,\n" +
                "primary key(id) not enforced" +
                ")" + SQLUtil.getUpsertKafkaDDL("dwd_trade_pay_suc_detail"));
        resultTable.executeInsert("dwd_trade_pay_suc_detail");

    }
}
