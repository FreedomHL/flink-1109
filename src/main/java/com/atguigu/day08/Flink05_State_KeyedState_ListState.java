package com.atguigu.day08;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Comparator;

public class Flink05_State_KeyedState_ListState {
    public static void main(String[] args) throws Exception {
        //TODO 针对每个传感器输出最高的3个水位值

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
                .process(new MyKeyed05(3));

        //4.打印
        result.print("main...");

        //5.执行
        env.execute();
    }

    public static class MyKeyed05 extends KeyedProcessFunction<String, WaterSensor, WaterSensor> {

        //1.定义状态变量
        private ListState<WaterSensor> listState;

        private int topN;

        public MyKeyed05(){

        }

        public MyKeyed05(int topN){
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //2.初始化状态变量

            listState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<WaterSensor>("list-status",WaterSensor.class));
        }


        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            listState.add(value);

            ArrayList<WaterSensor> list = Lists.newArrayList(listState.get().iterator());

            list.sort(new Comparator<WaterSensor>() {
                @Override
                public int compare(WaterSensor o1, WaterSensor o2) {
                    //return (o1.getVc() - o2.getVc())>0?-1:1;
                    return o2.getVc().compareTo(o1.getVc());
                }
            });

            //保留所有历史数据
            /*for (int i = 0; i < Math.min(topN,list.size()); i++) {
                out.collect(list.get(i));
            }*/

            //输出：只保留TopSize的数据
            if(list.size() > topN){
                list.remove(topN);
                listState.update(list);
            }

            for (WaterSensor waterSensor : list) {
                out.collect(waterSensor);
            }

        }
    }
}
