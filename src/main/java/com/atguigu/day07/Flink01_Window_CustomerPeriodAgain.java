package com.atguigu.day07;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink01_Window_CustomerPeriodAgain {
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

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = waterSensorDS.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                //延迟2000L
                return new MyPerid2(2000L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            //返回时间戳  成为13位
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));

        waterSensorSingleOutputStreamOperator.keyBy(data->data.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("vc")
                .print();
        env.execute();

    }

    public static class MyPerid2 implements WatermarkGenerator<WaterSensor>{
        private long orderness;
        private long maxTs;

        public MyPerid2(long orderness){
            this.orderness = orderness;
            maxTs = Long.MIN_VALUE + orderness + 1;
        }

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            //每输入一条数据，得到最大的时间戳
            maxTs = Math.max(maxTs,eventTimestamp);
        }

        //返回wm
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTs - orderness - 1));
        }
    }
}
