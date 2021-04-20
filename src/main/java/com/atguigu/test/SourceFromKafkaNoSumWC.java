package com.atguigu.test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class SourceFromKafkaNoSumWC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"hh");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        DataStreamSource<String> dataSource = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = s.split(" ");
                for (int i = 0; i < words.length; i++) {
                    collector.collect(Tuple2.of(words[0], 1L));
                }
            }
        });

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = flatMap.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
            }
        });
        result.print();


        env.execute();
    }
}
