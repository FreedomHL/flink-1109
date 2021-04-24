package com.atguigu.day08;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink08_State_KeyedState_MapState {
    public static void main(String[] args) throws Exception {
        //TODO 去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
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
                .process(new MyKeyed08());

        //4.打印
        result.print("main...");

        //5.执行
        env.execute();
    }
    public static class MyKeyed08 extends KeyedProcessFunction<String,WaterSensor,WaterSensor>{
        private MapState<Double,String> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<Double, String>("map-status",Double.class,String.class));

        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            if(!mapState.contains(value.getVc())){
                mapState.put(value.getVc(),value.getId());
                out.collect(value);
            }
        }
    }
}
