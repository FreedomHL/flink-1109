package com.atguigu.day07;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink04_Process_SideOutPut {
    public static void main(String[] args) throws Exception {
        //TODO processFunction实现分流功能   ---侧输出流

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取端口数据，并转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999).map(line -> {
            String[] words = line.split(",");
            return new WaterSensor(words[0], Long.valueOf(words[1]), Double.valueOf(words[2]));
        });

        //3.使用ProcessFunction实现分流功能  用不用KeyBy?
        SingleOutputStreamOperator<WaterSensor> processDS = waterSensorDS
                .keyBy(data -> data.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        if (value.getVc() < 40) {
                            ctx.output(new OutputTag<String>("sideOut") {
                            }, value.getId() + ":" + "水位：" + value.getVc());
                        }else{
                            out.collect(value);
                        }


                    }
                });

        //4.打印主流和侧输出流数据
        processDS.print("main<<<");
        processDS.getSideOutput(new OutputTag<String>("sideOut"){}).print("side......:");

        //5.启动
        env.execute();
    }
}
