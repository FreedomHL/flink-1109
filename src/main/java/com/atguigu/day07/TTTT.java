package com.atguigu.day07;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class TTTT {
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

        //3.提取时间戳生成wm
        SingleOutputStreamOperator<WaterSensor> waterSenorMK = waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        //为啥要分组？
        waterSenorMK.keyBy(data->data.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        TimerService timerService = ctx.timerService();

                        System.out.println();
                        timerService.registerEventTimeTimer(ctx.timestamp() + 5000);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                        System.out.println("定时器触发");
                    }
                }).print();


        env.execute();


    }
}
