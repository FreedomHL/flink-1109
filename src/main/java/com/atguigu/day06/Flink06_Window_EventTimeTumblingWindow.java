package com.atguigu.day06;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink06_Window_EventTimeTumblingWindow {
    public static void main(String[] args) throws Exception {
        //TODO 了解，0延迟事件时间
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketStream = env.socketTextStream("hadoop102", 9999);

        //3.将每行数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketStream.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] words = value.split(",");

                out.collect(new WaterSensor(words[0], Long.valueOf(words[1]), Double.valueOf(words[2])));
            }
        });

        //4.提取时间戳生成WaterMark                                                                                                //指定生成策略为0延迟
        SingleOutputStreamOperator<WaterSensor> waterSensorWithWMDS = waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                //分配时间戳
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override                                       //默认值
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L; //因为我的数据时间戳为10位，正常是13位
                    }
                }));

        //老版本
        waterSensorDS.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<WaterSensor>() {
            @Override
            public long extractAscendingTimestamp(WaterSensor element) {
                return element.getTs() * 1000L;
            }
        });


        //5.按照传感器Id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorWithWMDS.keyBy(WaterSensor::getId);

        //6.开窗                                                                          //事件时间
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //7.聚合
        SingleOutputStreamOperator<WaterSensor> result = windowedStream.sum("vc");

        //8.打印
        result.print();

        //9.启动
        env.execute();
    }
}
