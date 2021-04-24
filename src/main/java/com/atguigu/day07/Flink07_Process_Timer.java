package com.atguigu.day07;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink07_Process_Timer {
    public static void main(String[] args) throws Exception {
        //TODO 监控水位传感器的水位值，如果水位值在五分钟之内(processing time)连续上升，则报警。
        // 【idea测试：连续5s水位没有下降,且将报警信息写入侧输出流】
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取端口数据，并转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999).map(line -> {
            String[] words = line.split(",");
            return new WaterSensor(words[0], Long.valueOf(words[1]), Double.valueOf(words[2]));
        });


        SingleOutputStreamOperator<WaterSensor> resultDS = waterSensorDS.keyBy(data -> data.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

                    //获取上一次水位线
                    Double lastVc = Double.MIN_VALUE;

                    //保存上一次的定时器时间
                    Long tsTime = 0L;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                        out.collect(value);

                        //获取当前水位线
                        Double vc = value.getVc();

                        //获取定时器
                        TimerService timerService = ctx.timerService();

                        //获取当前处理时间
                        long ts = timerService.currentProcessingTime();


                        if (vc >= lastVc && tsTime == 0L) {
                            //水位线上升,注册定时器

                            timerService.registerProcessingTimeTimer(ts + 5000L);

                            //保存此定时器的时间
                            tsTime = ts + 5000L;
                            //将此次水位线 置为上一次水位线
                            lastVc = vc;

                        } else if (vc < lastVc) {
                            //水位线下降，删除定时器
                            timerService.deleteProcessingTimeTimer(tsTime);
                            //将此次水位线 置为上一次水位线
                            lastVc = vc;
                            //重置定时时间,来保证5s内只报警一次
                            tsTime = 0L;
                        }


                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                        ctx.output(new OutputTag<String>("side") {
                        }, "水位线连续上升报警！" + ctx.getCurrentKey());

                        tsTime = 0L;
                    }
                });

        resultDS.print("main...");

        resultDS.getSideOutput(new OutputTag<String>("side") {
        }).print("side>>>>>>>>>>>");

        env.execute();
    }
}
