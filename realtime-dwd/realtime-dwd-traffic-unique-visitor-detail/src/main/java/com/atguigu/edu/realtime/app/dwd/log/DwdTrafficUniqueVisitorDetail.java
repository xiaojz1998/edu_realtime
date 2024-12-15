package com.atguigu.edu.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.realtime.common.base.baseApp;
import com.atguigu.edu.realtime.common.constant.Constant;
import com.atguigu.edu.realtime.common.util.DateFormatUtil;
import com.atguigu.edu.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Title: DwdTrafficUniqueVisitorDetail
 * Create on: 2024/12/15
 *
 * @author zhengranran
 * @version 1.0.0
 * Description:
 *    任务：流量域独立访客事务事实表，过滤页面数据中的独立访客访问记录。
 *    需要启动进程：zk，kf，flume，DwdBaseLog，DwdTrafficUniqueVisitorDetail
 */
public class DwdTrafficUniqueVisitorDetail extends baseApp {
    public static void main(String[] args) {
        new DwdTrafficUniqueVisitorDetail().start(
                10012,4,"dwd_traffic_unique_visitor_detail", Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //todo 1.将从 kafka dwd_traffic_page中读到的日志数据做转化     -----jsonStr -> JSONObject
        //kafkaStrDS.print(); 能从kafka中消费到页面数据
        SingleOutputStreamOperator<JSONObject> firstPageObjDS  = kafkaStrDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String jsonStr) throws Exception {
                        return JSON.parseObject(jsonStr);
                    }
                });
        //firstPageObjDS.print();
        //todo 2.过滤last_page_id 不为 null 的数据     -----使用filter  目的：只要设备登录的数据
        SingleOutputStreamOperator<JSONObject> filterPageObjDS = firstPageObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                }
        );

        //todo 3.按照mid分组    ------keyBy
        KeyedStream<JSONObject, String> keyByObjDS
                = filterPageObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //keyByObjDS.print();
        //todo 4.利用状态过滤掉当天已经访问过的Mid     -----process(KeyedProcessFunction)  rich  open
           // todo (小细节：状态怎么过滤呢？不使用KeyedProcessFunction，使用filter  RichFilterFunction过滤今天访问过的访客)
        SingleOutputStreamOperator<JSONObject> filteredObjDS = keyByObjDS.filter(
                new RichFilterFunction<JSONObject>() {
                    //声明一个单值状态，用来存储时间
                    ValueState<String> lastVisitDate;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastVisitDateDesc", String.class);
                        //设置状态类型为创建和写，状态失效时间是1天
                        valueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1))
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .build()
                        );
                        lastVisitDate = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        //分别从状态和数据中获取日期
                        String lastVisitDateValue = lastVisitDate.value();
                        Long ts = jsonObject.getLong("ts");
                        String currentDate = DateFormatUtil.tsToDateTime(ts);

                        //判断，将最后访问日期更新到状态中
                        if (StringUtils.isEmpty(lastVisitDateValue) || DateFormatUtil.dateTimeToTs(currentDate) > DateFormatUtil.dateTimeToTs(lastVisitDateValue)) {
                            lastVisitDate.update(currentDate);
                            return true;
                        }
                        return false;
                    }
                }
        );
        filteredObjDS.print();
        //todo 5.提取字段并写入到kafka独立访客主题
        filteredObjDS.map(jsonObject -> jsonObject.toJSONString()).sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UNIQUE_VISITOR_DETAIL));

    }
}
