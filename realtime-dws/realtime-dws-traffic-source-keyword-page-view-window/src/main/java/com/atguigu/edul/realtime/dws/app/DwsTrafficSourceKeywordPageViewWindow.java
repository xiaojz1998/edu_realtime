package com.atguigu.edul.realtime.dws.app;

import com.atguigu.edu.realtime.common.base.BaseSQLAPP;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.SQLUtil;
import com.atguigu.edul.realtime.dws.function.KeywordUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Title: DwsTrafficSourceKeywordPageViewWindow
 * Create on: 2024/12/16
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *  搜索关键词聚合统计
 *  需要启动的进程
 *       zk、kafka、flume、Doris、DwdBaseLog、DwsTrafficSourceKeywordPageViewWindow
 */
public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLAPP {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(10021,4,"dws_traffic_source_keyword_page_view_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
        //TODO 1.注册自定义函数到表执行环境中
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class); //自定义函数名字：ik_analyze
        //TODO 2.从kafka的页面日志事实表中读取数据创建动态表   并指定Watermark的生成策略以及提取事件时间字段
        tableEnv.executeSql("CREATE TABLE page_log (\n" +
                "  `common` map<string,string>,\n" +
                "  `page` map<string,string>,\n" +
                "  `ts` bigint,\n" +
                "  et as TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  WATERMARK FOR et AS et\n" +
                ") " + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));

        //tableEnv.executeSql("select * from page_log").print();
        //TODO 3.过滤出搜索行为
        Table searchTable = tableEnv.sqlQuery("select\n" +
                "    page['item'] fullword,\n" +
                "    et\n" +
                "from page_log where page['item_type']='keyword' \n" +
                "and page['item'] is not null");
        tableEnv.createTemporaryView("search_table",searchTable);
        //searchTable.execute().print();  这里没有问题
        //TODO 4.分词   并用分词结果和原表进行join
        Table splitTable = tableEnv.sqlQuery("SELECT keyword,et\n" +
                "FROM search_table,LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        tableEnv.createTemporaryView("split_table",splitTable);
        //tableEnv.executeSql("select * from split_table").print();
        //TODO 5.分组、开窗、聚合计算
        //5.2 TVF聚合
        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "    DATE_FORMAT(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "    keyword,\n" +
                "    count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "    TUMBLE(TABLE split_table, DESCRIPTOR(et), INTERVAL '10' second))\n" +
                "  GROUP BY window_start, window_end,keyword");
        //resTable.execute().print();
        //TODO 6.将聚合的结果写到Doris表中
        //6.1 创建动态表和要写入的Doris表进行映射
        tableEnv.executeSql("CREATE TABLE dws_traffic_source_keyword_page_view_window (\n" +
                "    stt string,\n" +
                "    edt string,\n" +
                "    cur_date string,\n" +
                "    keyword string,\n" +
                "    keyword_count bigint\n" +
                "    ) " + SQLUtil.getDorisDDL("dws_traffic_source_keyword_page_view_window"));
        //6.2 写入
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}
