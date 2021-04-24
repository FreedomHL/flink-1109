package com.atguigu.day08;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_State_KeyedState_AggState {
    public static void main(String[] args) throws Exception {
        //TODO 计算每个传感器的平均水位
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
        SingleOutputStreamOperator<Tuple2<String, Double>> result = waterSensorDS.keyBy(WaterSensor::getId)
                .process(new MyKeyed07());

        //4.打印
        result.print("main...");

        //5.执行
        env.execute();
    }

    public static class MyKeyed07 extends KeyedProcessFunction<String,WaterSensor,Tuple2<String,Double>>{

        //1.定义属性
        private AggregatingState<Double,Double> aggregatingState;

        //2.初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Double, Acc, Double>("agg-status",
                    new AggregateFunction<Double, Acc, Double>() {
                        @Override
                        public Acc createAccumulator() {
                            return new Acc(0,0.0D);
                        }


                        //中间值，缓冲
                        @Override
                        public Acc add(Double value, Acc accumulator) {
                            return new Acc(accumulator.getCount() + 1,value + accumulator.getSum());
                        }

                        @Override
                        public Double getResult(Acc accumulator) {
                            return accumulator.getSum() / (accumulator.getCount() * 1.0);
                        }

                        @Override
                        public Acc merge(Acc a, Acc b) {
                            return new Acc(a.getCount() + b.getCount(),a.getSum() + b.getSum());
                        }
                    },Acc.class));
        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
            aggregatingState.add(value.getVc());

            out.collect(Tuple2.of(value.getId(),aggregatingState.get()));
        }
    }
}
