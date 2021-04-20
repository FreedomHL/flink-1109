package com.atguigu.day07;
import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
public class Fink07_TeacherCode {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.getConfig().setAutoWatermarkInterval(5000L);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });

        //3.按照传感器分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(WaterSensor::getId);

        //4.使用ProcessFunction实现水位线连续5秒不下降,则报警至侧输出流
        SingleOutputStreamOperator<WaterSensor> processDS = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            //定义上一次的水位线
            private Double lastVc = Double.MIN_VALUE;

            //定义时间用于保存注册定时器的时间
            private Long timerTs = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                //输出数据到主流
                out.collect(value);

                TimerService timerService = ctx.timerService();

                //取出当前数据中的水位线
                Double vc = value.getVc();

                //如果为第一条数据,则注册5秒以后的定时器
//                if (lastVc == Double.MIN_VALUE) {
//
//                    //注册定时器
//                    long ts = timerService.currentProcessingTime();
//                    timerTs = ts + 5000L;
//                    timerService.registerProcessingTimeTimer(timerTs);
//
//                    //修改上一次水位线
//                    lastVc = vc;
//                } else
                if (vc < lastVc) { //水位线下降
                    //删除定时器
                    timerService.deleteProcessingTimeTimer(timerTs);
                    //修改上一次水位线
                    lastVc = vc;
                    //重置时间戳
                    timerTs = Long.MIN_VALUE;
                } else if (vc >= lastVc && timerTs == Long.MIN_VALUE) {
                    //注册定时器
                    long ts = timerService.currentProcessingTime();
                    timerTs = ts + 5000L;
                    timerService.registerProcessingTimeTimer(timerTs);

                    //修改上一次水位线
                    lastVc = vc;
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {

                ctx.output(new OutputTag<String>("Side") {
                }, ctx.getCurrentKey() + "传感器连续5秒水位没有下降！");

                //重置时间戳
                timerTs = Long.MIN_VALUE;

            }
        });

        //5.打印
        processDS.print("Main>>>>>>>>>>>>");
        processDS.getSideOutput(new OutputTag<String>("Side") {
        }).print("Side>>>>>>>>>>>>");

        //6.启动
        env.execute();

    }
}
