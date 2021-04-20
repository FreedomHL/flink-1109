package com.atguigu.day06;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Flink03_Window_ReduceAndWindowFunc {
    public static void main(String[] args) throws Exception {
        //TODO 使用reduce增量聚合和window全量聚合，测试结合使用增量和全量函数
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDS = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (int i = 0; i < words.length; i++) {
                    out.collect(Tuple2.of(words[i], 1L));
                }
            }
        });

        //4.按照单词分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);

        //5.开窗
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //6.聚合   结合增量聚合函数和全量聚合函数的优点,来一条聚合一条，并且能拿到窗口信息
        // 其中全量聚合函数的集合中，原本需要存储所有的(xx,1)的，浪费了内存资源
        // 现在只会存储聚合后的结果
        SingleOutputStreamOperator<Tuple3<Long, String, Long>> reduceDS = windowedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        }, new WindowFunction<Tuple2<String, Long>, Tuple3<Long, String, Long>, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple3<Long, String, Long>> out) throws Exception {
                Tuple2<String, Long> next = input.iterator().next();
                out.collect(Tuple3.of(window.getStart(), next.f0, next.f1));
            }
        });

        //7.打印
        reduceDS.print();

        //8.启动
        env.execute();
    }
}
