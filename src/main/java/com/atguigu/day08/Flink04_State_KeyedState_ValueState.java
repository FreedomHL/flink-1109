package com.atguigu.day08;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink04_State_KeyedState_ValueState {
    public static void main(String[] args) throws Exception {
        //TODO 检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警到侧输出流。

        //问题：1.并行度有无关系？我觉得是有关系的

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读入数据并转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(data -> {
                    String[] words = data.split(",");
                    return new WaterSensor(words[0], Long.valueOf(words[1]), Double.valueOf(words[2]));
                });

                            //侧输出流：不算关窗后，无法进入滚动区间的数据
        //3.分组，并使用RichFunction 或 [ProcessFunction(定时器和侧输出流必用)]
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.keyBy(WaterSensor::getId)
                .process(new MyKeyedProcessedFun());

        //4.打印
        result.print("main...");
        result.getSideOutput(new OutputTag<String>("side"){}).print("side");

        //5.执行
        env.execute();
    }
    public static class MyKeyedProcessedFun extends KeyedProcessFunction<String,WaterSensor,WaterSensor> {

        //1.定义状态
        private ValueState<Double> valueState;

        //2.初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<Double>("value-state",Double.class));

        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            //获取状态中的数据
            Double lastVC = valueState.value();

            if(lastVC != null && Math.abs(lastVC - value.getVc()) >= 10.0D){
                ctx.output(new OutputTag<String>("side"){},"连续的两个水位线差值超过10，就输出报警到侧输出流" + value);
            }
            out.collect(value);

            //保存上一次的状态
            valueState.update(value.getVc());

        }
    }
}
