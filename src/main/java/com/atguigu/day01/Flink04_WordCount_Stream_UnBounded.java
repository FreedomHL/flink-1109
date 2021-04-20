package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//流处理中的无界流
public class Flink04_WordCount_Stream_UnBounded {
    public static void main(String[] args) throws Exception {
        //1、获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 设置线程数
        env.setParallelism(1);
        //2、读取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        //3、flatMap将数据拆分并组成Tuple元组
        SingleOutputStreamOperator<String> wordToOneDS = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Long>> map = wordToOneDS.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                return Tuple2.of(s, 1L);
            }
        }).setParallelism(2);

        //4、聚合
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = map.keyBy(0);
        //5、计算
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);
        //6、打印
        sum.print();
        env.execute();
    }

}
