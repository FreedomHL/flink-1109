package com.atguigu.day04;

import com.atguigu.day04.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class PVTest_Process {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStreamSource<String> ds = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<Tuple2<UserBehavior, Long>> flatMap = ds.flatMap(new FlatMapFunction<String, Tuple2<UserBehavior, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<UserBehavior, Long>> out) throws Exception {
                String[] words = value.split(",");

                UserBehavior userBehavior = new UserBehavior(Long.valueOf(words[0]), Long.valueOf(words[1]), Integer.valueOf(words[2]), words[3], Long.valueOf(words[4]));

                if ("pv".equals(userBehavior.getBehavior())) {
                    out.collect(Tuple2.of(userBehavior,1L));
                }
            }
        });


        flatMap.keyBy(1).sum(1).print();

        env.execute();
    }
}
