package com.atguigu.day07;

import com.atguigu.day03.bean.WaterSensor;
import com.atguigu.day04.bean.OrderEvent;
import com.atguigu.day04.bean.TxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink08_JoinFunction {
    public static void main(String[] args) throws Exception {
        //TODO 使用flink join 实现双流join,匹配 支付订单和到账数据

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取支付流数据
        DataStreamSource<String> orderLogStrDS = env.readTextFile("input/OrderLog.csv");

        //3.读取到账流数据
        DataStreamSource<String> receiptStrDS = env.readTextFile("input/ReceiptLog.csv");

        //4.将两个流转化为JavaBean对象
        SingleOutputStreamOperator<OrderEvent> orderEventDS = orderLogStrDS.map(line -> {
            String[] words = line.split(",");
            return new OrderEvent(Long.valueOf(words[0]), words[1], words[2], Long.valueOf(words[3]));
        }).filter(data->data.getTxId() != null)
                //必须提取事件时间
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                }));

        SingleOutputStreamOperator<TxEvent> receiptEventDS = receiptStrDS.map(line -> {
            String[] words = line.split(",");
            return new TxEvent(words[0], words[1], Long.valueOf(words[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<TxEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<TxEvent>() {
            //必须提取事件时间
            @Override
            public long extractTimestamp(TxEvent element, long recordTimestamp) {
                return element.getEventTime() * 1000L;
            }
        }));


        //5.必须keyBy,intervalJoin，并使用processFunction
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = orderEventDS.keyBy(data -> data.getTxId())
                .intervalJoin(receiptEventDS.keyBy(data -> data.getTxId()))
                //.inEventTime()
                .between(Time.seconds(-10), Time.seconds(10))
                .process(new ProcessJoinFunction<OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>() {

                    @Override
                    public void processElement(OrderEvent left, TxEvent right, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
                        out.collect(Tuple2.of(left, right));
                    }
                });

        result.print();

        env.execute();
    }


}
