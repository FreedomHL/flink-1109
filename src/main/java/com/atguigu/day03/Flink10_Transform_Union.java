package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink10_Transform_Union {
    public static void main(String[] args) throws Exception {
        //TODO UNION 可以多条流，但是类型必须一致
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds1 = env.socketTextStream("hadoop102", 9999);

        DataStreamSource<String> ds22 = env.socketTextStream("hadoop102", 8888);

        DataStream<String> union = ds1.union(ds22);

        union.print();

        env.execute();
    }
}
