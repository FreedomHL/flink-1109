package com.atguigu.day08;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Flink03_State_KeyedState {
    public static void main(String[] args) throws Exception {
        //TODO 并不用来执行，而是看看有哪些状态管理
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.使用RichFunction或者ProcessFunction
        socketTextStream.keyBy(data->data.split(",")[0])
                .process(new MyKeyedProcessFunction());


        env.execute();
    }
    public static class MyKeyedProcessFunction extends KeyedProcessFunction<String,String,String>{

        // 1.定义状态
        private ValueState<String> valueState;
        private ListState<String> listState;
        private MapState<String,String> mapState;
        private ReducingState<String> reducingState;
        private AggregatingState<String,String> aggregatingState;

        //2.初始化状态
        // 生命周期方法
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext()
                    .getState(new ValueStateDescriptor<String>("value_state",String.class));

            listState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<String>("list-state",String.class));

            mapState = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<String, String>("map-state",String.class,String.class));

            reducingState = getRuntimeContext()
                    .getReducingState(new ReducingStateDescriptor<String>("reduce-state", new ReduceFunction<String>() {
                        @Override
                        public String reduce(String value1, String value2) throws Exception {
                            return null;
                        }
                    },String.class));

            aggregatingState = getRuntimeContext()
                    .getAggregatingState(new AggregatingStateDescriptor<String, Object, String>("agg-state", new AggregateFunction<String, Object, String>() {
                        @Override
                        public Object createAccumulator() {
                            return null;
                        }

                        @Override
                        public Object add(String value, Object accumulator) {
                            return null;
                        }

                        @Override
                        public String getResult(Object accumulator) {
                            return null;
                        }

                        @Override
                        public Object merge(Object a, Object b) {
                            return null;
                        }
                    },Object.class));
        }


        //3.使用状态
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

            //3.1 ValueState
            String value1 = valueState.value();//读
            valueState.update(value);//写
            valueState.clear();//删

            //3.2 ListState
            listState.add(value);
            listState.addAll(new ArrayList<>());
            Iterable<String> strings = listState.get();
            listState.update(new ArrayList<>());
            listState.clear();

            //3.3 MapState
            mapState.contains("");
            mapState.isEmpty();
            mapState.put("","");
            mapState.putAll(new HashMap<>());
            mapState.get("");
            Iterable<Map.Entry<String, String>> entries = mapState.entries();
            mapState.keys();
            mapState.values();
            mapState.clear();;
            mapState.remove("");

            //3.4 ReduceState
            String s = reducingState.get();
            reducingState.add(value);
            reducingState.clear();

            //3.5 AggState
            aggregatingState.add(value);
            String s1 = aggregatingState.get();
            aggregatingState.clear();
        }
    }
}
