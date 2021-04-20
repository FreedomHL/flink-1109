package com.atguigu.day03;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink05_Transform1_RichMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        //开启的话看并行度，
        //关闭：
        //  无界流，只会调用一次关闭的方法
        //  有界流的关闭，会被关闭两次，一次是关闭别人，另一次是关闭自己
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 9999);

        //DataStreamSource<String> ds = env.readTextFile("input/sensor.txt");

        SingleOutputStreamOperator<WaterSensor> map = ds.map(new MyRichMap());

        map.print();

        env.execute();

    }
    public static class MyRichMap extends RichMapFunction<String, WaterSensor>{
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open..");
        }

        @Override
        public WaterSensor map(String s) throws Exception {
            String[] words = s.split(",");
            return new WaterSensor(words[0],Long.valueOf(words[1]),Double.valueOf(words[2]));
        }

        @Override
        public void close() throws Exception {
            System.out.println("close..");
        }
    }
}
