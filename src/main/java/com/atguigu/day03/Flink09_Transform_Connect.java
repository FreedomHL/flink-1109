package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class Flink09_Transform_Connect {
    public static void main(String[] args) throws Exception {
        //TODO connect connect的两条流类型可以不一致
        //最多只能两条流connect

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds1 = env.socketTextStream("hadoop102", 9999);

        DataStreamSource<String> ds22 = env.socketTextStream("hadoop102", 8888);

        SingleOutputStreamOperator<Integer> ds2 = ds22.map(data -> data.length());

        ConnectedStreams<String, Integer> connect = ds1.connect(ds2);

        SingleOutputStreamOperator<Object> result = connect.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String value) throws Exception {
                return value;
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value;
            }
        });

        result.print();

        env.execute();
    }
}
