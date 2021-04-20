package com.atguigu.test;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.HashMap;
import java.util.HashSet;

public class SourceFromPortAddtoSet {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> dataSource = env.socketTextStream("hadoop102", 9999);

        //这里写是并行的
        HashSet<String> set = new HashSet<>();

        SingleOutputStreamOperator<String> flatMap = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (int i = 0; i < words.length; i++) {
                    if (!set.contains(words[i])){
                        set.add(words[i]);
                        collector.collect(words[i]);
                    }
                }
            }
        });
        flatMap.keyBy(x->x).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                if (!set.contains(s)){
                    set.add(s);
                    return true;
                }
                return false;
            }
        }).print();



        env.execute();
    }
}
