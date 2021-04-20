package com.atguigu.day06;

import org.apache.flink.api.common.functions.AggregateFunction;
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

public class Flink04_Window_AggAndWindowFunc {
    public static void main(String[] args) throws Exception {
        //TODO 结合增量聚合函数的优点和全量聚合函数    使用Agg增量聚合
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
        // 现在只会存储聚合后的结果                     //输入，中间值，结果
        SingleOutputStreamOperator<Tuple3<Long, String, Long>> aggregateDS = windowedStream.aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {


            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(Tuple2<String, Long> value, Long accumulator) {
                return value.f1 + accumulator;
            }

            @Override
            public Long getResult(Long accumulator) {
                return accumulator;
            }
            //只在会话窗口有用，是两个累加器相加。会话窗口，每一个元素都会创建新的窗口,会话结束后，会合并这些窗口
            @Override
            public Long merge(Long a, Long b) {
                return a + b;
            }
        }, new WindowFunction<Long, Tuple3<Long, String, Long>, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<Tuple3<Long, String, Long>> out) throws Exception {
                //获取结果
                Long count = input.iterator().next();

                out.collect(Tuple3.of(window.getStart(), key, count));

            }
        });

        //7.打印
        aggregateDS.print();

        //8.启动
        env.execute();
    }
}
