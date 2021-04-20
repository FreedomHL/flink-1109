package com.atguigu.test;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;

public class Day07Test {
    public static void main(String[] args) throws Exception {
        //TODO 3.使用事件时间处理数据,
        // 编写代码从端口获取数据实现每隔5秒钟计算最近30秒的每个传感器发送温度的次数,
        // Watermark设置延迟2秒钟,允许迟到数据2秒钟,再迟到的数据放至侧输出流,
        // 通过观察结果说明什么样的数据会进入侧输出流。

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取端口中的数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.map转化为javaBean
        SingleOutputStreamOperator<WaterSensor> waterSenorDS = socketTextStream.map(line -> {
            String[] words = line.split(",");
            return new WaterSensor(words[0], Long.valueOf(words[1]), Double.valueOf(words[2]));
        });

        //4.设置watermark
        SingleOutputStreamOperator<WaterSensor> waterSensorWMDS = waterSenorDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        //5.按照传感器Id分组
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorWMDS.keyBy(data -> data.getId());

        //6.分组之后开窗 事件时间滚动
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = waterSensorStringKeyedStream
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<WaterSensor>("side"){});

        SingleOutputStreamOperator<Tuple3<Long,String, Long>> aggregateDS = windowedStream.aggregate(new AggregateFunction<WaterSensor, Long, Long>() {

            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(WaterSensor value, Long accumulator) {
                return accumulator + 1;
            }

            @Override
            public Long getResult(Long accumulator) {
                return accumulator;
            }

            @Override
            public Long merge(Long a, Long b) {
                return a + b;
            }
        }, new WindowFunction<Long, Tuple3<Long,String, Long>, String, TimeWindow>() {

            @Override
            public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<Tuple3<Long,String, Long>> out) throws Exception {
                out.collect(Tuple3.of(window.getStart(),key, input.iterator().next()));
            }
        });

        aggregateDS.print("agg");
        aggregateDS.getSideOutput(new OutputTag<WaterSensor>("side"){}).print(".............");

        //执行
        env.execute();
    }
}
