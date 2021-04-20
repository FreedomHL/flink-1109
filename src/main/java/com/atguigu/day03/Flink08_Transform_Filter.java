package com.atguigu.day03;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_Transform_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.readTextFile("input/sensor.txt");

        //普通filter
        //SingleOutputStreamOperator<String> result = ds.filter(new MyFilter());

        SingleOutputStreamOperator<String> result = ds.filter(new MyRichFilter());

        result.print();

        env.execute();
    }
    //普通filter
    public static class MyFilter implements FilterFunction<String>{


        @Override
        public boolean filter(String value) throws Exception {
            String[] split = value.split(",");
            return Double.valueOf(split[2]) > 42.5;
        }
    }
    public static class MyRichFilter extends RichFilterFunction<String>{
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open..");
        }

        @Override
        public boolean filter(String value) throws Exception {
            String[] split = value.split(",");
            return Double.valueOf(split[2]) > 42.5;
        }
    }
}
