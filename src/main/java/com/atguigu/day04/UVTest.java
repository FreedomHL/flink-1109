package com.atguigu.day04;

import com.atguigu.day04.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;

import java.util.HashSet;

public class UVTest {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStreamSource<String> ds = env.readTextFile("input/UserBehavior.csv");


        //多条并行度的前提下，这里的东西是所有线程都会执行的
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                    String[] split = value.split(",");
                    if("pv".equals(split[3]))
                        out.collect(split[0]);
            }
        });

        KeyedStream<String, String> stringStringKeyedStream = stringSingleOutputStreamOperator.keyBy(s -> "uv");

        SingleOutputStreamOperator<Long> result = stringStringKeyedStream.process(new KeyedProcessFunction<String, String, Long>() {
            //因为keyBy(s -> "uv")，所以这边只是一个分区
            private Long cnt = 0L;
            private HashSet<String> set = new HashSet<>();
            @Override
            public void processElement(String value, Context ctx, Collector<Long> out) throws Exception {
                if(!set.contains(value)){
                    set.add(value);
                    cnt++;
                    out.collect(cnt);
                }
            }
        });
        result.print();

        env.execute();
    }
}
