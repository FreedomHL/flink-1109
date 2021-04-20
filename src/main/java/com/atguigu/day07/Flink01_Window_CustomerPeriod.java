package com.atguigu.day07;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink01_Window_CustomerPeriod {
    public static void main(String[] args) throws Exception {
        //TODO 自定义周期性生成waterMark规则 2s延迟
        // 对5s内的水位线求和

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取端口内数据并转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999).flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] words = value.split(",");
                out.collect(new WaterSensor(words[0], Long.valueOf(words[1]), Double.valueOf(words[2])));
            }
        });

        //3.自定义周期性waterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDS.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPeriod(2000L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));

        //4.分组开窗聚合
        SingleOutputStreamOperator<WaterSensor> result = waterSensorSingleOutputStreamOperator.keyBy(data -> data.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("vc");

        result.print();

        env.execute();
    }
    public static class MyPeriod implements WatermarkGenerator<WaterSensor>{

        private long orderNess;     //延迟
        private long maxTimestamp;  //最大时间戳

        public MyPeriod(){

        }
        public MyPeriod(long orderNess){
            this.orderNess = orderNess;
            maxTimestamp = Long.MIN_VALUE + orderNess + 1;
        }

        @Override                               //当前这条数据 生成wm时提取的时间戳
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            //对于每一条Event来说，我的时间戳应该怎么变化，使当前时间为最大时间戳，保证wm单调递增
            maxTimestamp = Math.max(maxTimestamp,eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //周期性                                                    //延迟2s - 1ms
            output.emitWatermark(new Watermark(maxTimestamp - orderNess - 1 ));
        }
    }
}
