package com.atguigu.day05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Fink02_Window_ProcessTime_SlidingWindow {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从端口读取数据创建流
        DataStreamSource<String> textStream = env.socketTextStream("hadoop102", 9999);

        //3.压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDS = textStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = value.split(" ");
                for (String s : words) {
                    out.collect(Tuple2.of(s, 1L));
                }
            }
        });

        //4.按照单词分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);

        //5.开窗，按照时间的滑动窗口
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowedStream = keyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(6),Time.seconds(2)));

        //6.聚合计算
        SingleOutputStreamOperator<Tuple2<String, Long>> result = windowedStream.sum(1);

        //7.打印
        result.print();
        //8.启动任务

        env.execute();






    }
}
