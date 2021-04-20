package com.atguigu.day03;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink05_Transform2_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStreamSource<String> ds = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> ds = env.readTextFile("input/sensor.txt");
        SingleOutputStreamOperator<String> result = ds.flatMap(new MyFlatMap());

        result.print();

        env.execute();
    }
    public static class MyFlatMap extends RichFlatMapFunction<String,String>{

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open..");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] split = value.split(" ");
            for (int i = 0; i < split.length; i++) {
                out.collect(split[i]);
            }
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }
}
