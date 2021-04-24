package com.atguigu.day09;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink02_CK_WordCount {
    public static void main(String[] args) throws Exception {

        //设置访问hdfs的用户名
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink1109/ck"));

        //1.2开启ck
        env.enableCheckpointing(5000);//每5秒checkpoint一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(50000L); // Checkpoint 必须在50s内完成，否则就会被抛弃
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);//最大并发

        //2.读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102",9999);

        //3.切词,分组,聚合,打印
        socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (int i = 0; i < words.length; i++) {
                    out.collect(Tuple2.of(words[0],1));
                }
            }
        }).keyBy(data->data.f0)
                .sum(1).print();

        //4.启动
        env.execute();
    }
}
