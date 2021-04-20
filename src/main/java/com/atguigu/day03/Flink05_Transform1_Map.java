package com.atguigu.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink05_Transform1_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> ds = env.readTextFile("input/sensor.txt");

        ds.map(new MapFunction<String,Long>() {
            @Override
            public Long map(String s) throws Exception {
                return Long.valueOf(s.length());
            }
        }).print();


        env.execute();
    }
}
