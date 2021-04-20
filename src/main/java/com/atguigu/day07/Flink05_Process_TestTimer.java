package com.atguigu.day07;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_Process_TestTimer {
    public static void main(String[] args) throws Exception {
        //TODO 使用processFunction 测试定时器，实现处理时间 5s以后打印输出一句话
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取端口数据，并转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999).map(line -> {
            String[] words = line.split(",");
            return new WaterSensor(words[0], Long.valueOf(words[1]), Double.valueOf(words[2]));
        });

        //3.定时器必须keyBy
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.keyBy(data -> data.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        TimerService timerService = ctx.timerService();

                        //获取当前处理时间
                        long ts = timerService.currentProcessingTime();

                        //注册定时器
                        timerService.registerProcessingTimeTimer(ts + 5000L);

                        out.collect(value);

                    }

                    //定时器触发时 调用的方法
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {

                        System.out.println("5s过后，定时器触发！");
                    }
                });

        result.print();

        env.execute();
    }
}
