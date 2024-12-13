package com.atguigu.edu.realtime.dim.app;

import com.atguigu.edu.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Title: DimApp
 * Create on: 2024/12/14 1:55
 *
 * @author Xiao Jianzhe
 * @version 1.0.0
 * Description:
 *  Dim层开发
 *  需要启动的进程:
 *    zk、kafka、maxwell、hdfs、hbase、DimApp
 */
public class DimApp {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT,8088);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(4);

        //TODO 2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        /*

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //2.2 设置检查点超时时间
        checkpointConfig.setCheckpointTimeout(60000L);
        //2.3 设置job取消之后检查点是否保留
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.7 设置检查点存储路径
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");
        //2.8 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");

         */
        //TODO 3.从kafka主题中读取主流业务数据
        //3.2 创建消费者对象
        //FlinkSourceUtil.getKafkaSource()


    }
}
