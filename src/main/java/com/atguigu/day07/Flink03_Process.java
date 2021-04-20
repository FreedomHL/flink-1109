package com.atguigu.day07;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink03_Process {
    //TODO process用途   无脑使用
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("hadoop102", 9999)
                .process(new MyProcessFunc());

    }

    public static class MyProcessFunc extends ProcessFunction<String, String> {

        //1.声明周期方法
        @Override
        public void open(Configuration parameters) throws Exception {
            //运行时上下文环境
            RuntimeContext runtimeContext = getRuntimeContext();

            //2.状态编程
            //runtimeContext.getState();
        }

        @Override
        public void close() throws Exception {
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

            //主流数据
            out.collect("");

            //3.侧输出流
            //ctx.output();

            //4.定时器
            TimerService timerService = ctx.timerService();
            long ts = timerService.currentProcessingTime();
            timerService.registerProcessingTimeTimer(ts + 5000L);
            timerService.deleteProcessingTimeTimer(ts + 5000L);
        }

        //定时器触发
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

        }
    }
}
