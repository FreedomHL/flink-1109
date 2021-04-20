package com.atguigu.day03;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink13_Transform_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> flatMapProcess = ds.process(new FlatMapProcess());

        SingleOutputStreamOperator<Tuple2<String, Long>> mapProcess = flatMapProcess.process(new MapProcess());

        SingleOutputStreamOperator<Tuple2<String, Long>> result = mapProcess.keyBy(data->data.f0).sum(1);

        result.print();

        env.execute();
    }
    public static class FlatMapProcess extends ProcessFunction<String,String>{

        //生命周期方法
        @Override
        public void open(Configuration parameters) throws Exception {

        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

            //运行时上下文，状态编程
            RuntimeContext runtimeContext = getRuntimeContext();

            String[] s = value.split(" ");
            for (int i = 0; i < s.length; i++) {
                out.collect(s[i]);
            }

            //定时器
            TimerService timerService = ctx.timerService();
            timerService.registerEventTimeTimer(2000);

            //获取当前处理数据的时间（进入当前方法的时间）
            timerService.currentProcessingTime();

            //事件时间
            timerService.currentWatermark();

            //侧输出流
            //ctx.output();
        }
    }

    public static class MapProcess extends ProcessFunction<String, Tuple2<String,Long>> {


        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
            out.collect(Tuple2.of(value,1L));
        }
    }


}
