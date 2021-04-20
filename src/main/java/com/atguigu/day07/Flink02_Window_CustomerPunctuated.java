package com.atguigu.day07;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink02_Window_CustomerPunctuated {
    public static void main(String[] args) throws Exception {
        //TODO 自定义间歇性生成watermark
        // 对5s内的水位线求和
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取端口中的数据，并转化为样例类
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = streamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] words = value.split(",");
                out.collect(new WaterSensor(words[0], Long.valueOf(words[1]), Double.valueOf(words[2])));
            }
        });

        //3.提取时间戳生成watermark
        SingleOutputStreamOperator<WaterSensor> waterSensorMKDS = waterSensorDS.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPunctuated(2000L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));

        //4.分组
        waterSensorMKDS.keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("vc")
                .print();

        //5.开窗
        //

        //6.打印

        //7.执行
        env.execute();
    }
    public static class MyPunctuated implements WatermarkGenerator<WaterSensor>{

        private long orderNess;

        private long maxTs;

        public MyPunctuated(){

        }
        public MyPunctuated(long orderNess){
            this.orderNess = orderNess;
            maxTs = Long.MIN_VALUE + orderNess + 1;
        }

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(maxTs,eventTimestamp);
            //来一条处理一次
            output.emitWatermark(new Watermark(maxTs - orderNess - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }

}
