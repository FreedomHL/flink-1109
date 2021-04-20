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

public class Flink02_Window_WindowFunc {
    public static void main(String[] args) throws Exception {
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
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(3)));

        //6.聚合  全量     //WindowFunction<IN, OUT, KEY, W extends Window>
        SingleOutputStreamOperator<Tuple3<Long, String, Long>> applyDS = windowedStream.apply(new WindowFunction<Tuple2<String, Long>, Tuple3<Long, String, Long>, String, TimeWindow>() {

            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple3<Long, String, Long>> out) throws Exception {
                ArrayList<Tuple2<String, Long>> list = Lists.newArrayList(input.iterator());
                Long size = Long.valueOf(list.size());
                out.collect(Tuple3.of(window.getStart(), key, size));
            }
        });

        //7.打印
        applyDS.print();

        //8.启动
        env.execute();
    }
}
