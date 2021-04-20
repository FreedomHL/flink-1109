package com.atguigu.day07;

import com.atguigu.day03.bean.WaterSensor;
import com.sun.org.apache.xalan.internal.xsltc.dom.SimpleResultTreeImpl;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink06_Process_TestTimerOnEventTime {
    public static void main(String[] args) throws Exception {
        //TODO 使用processFunction 测试定时器,使用事件时间  5s以后，定时器触发

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取端口数据并转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999).map(line -> {
            String[] words = line.split(",");
            return new WaterSensor(words[0], Long.valueOf(words[1]), Double.valueOf(words[2]));
        });

        //3.提取时间戳生成waterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorWithWMDS = waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        //4.分组 并使用process 注册定时器
        SingleOutputStreamOperator<WaterSensor> result = waterSensorWithWMDS.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                        //注册定时器
                        TimerService timerService = ctx.timerService();

                        long ts = timerService.currentWatermark();
                        System.out.println("当前watermark时间：" + ts);

                        Long ts1 = value.getTs() * 1000L;

                        timerService.registerEventTimeTimer(ts1 + 5000L - 1); //设置定时器时间
                        //如果定时器不 - 1 ，则 定时器此时为 6000，而wm时间仅5999

                        out.collect(value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {

                        System.out.println("事件时间5s已过!");
                    }
                });

        //5.打印
        result.print();
        //6.执行
        env.execute();
    }
}
