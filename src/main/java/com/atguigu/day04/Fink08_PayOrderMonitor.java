package com.atguigu.day04;

import com.atguigu.day04.bean.OrderEvent;
import com.atguigu.day04.bean.TxEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Fink08_PayOrderMonitor {
    public static void main(String[] args) throws Exception {
        //TODO 订单支付实时对账
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取订单数据
        DataStreamSource<String> orderLogDS = env.readTextFile("input/OrderLog.csv");

        //读取交易数据
        DataStreamSource<String> receiptLogDS = env.readTextFile("input/ReceiptLog.csv");

        //转换为订单类
        KeyedStream<OrderEvent, Tuple> orderEventDS = orderLogDS.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                String[] words = value.split(",");
                if (!"".equals(words[2])) {
                    out.collect(new OrderEvent(Long.valueOf(words[0]), words[1], words[2],
                            Long.valueOf(words[3])));
                }
            }
        }).keyBy("txId");

        KeyedStream<TxEvent, Tuple> receiptEvent = receiptLogDS.flatMap(new FlatMapFunction<String, TxEvent>() {
            @Override
            public void flatMap(String value, Collector<TxEvent> out) throws Exception {
                String[] words = value.split(",");
                out.collect(new TxEvent(words[0], words[1], Long.valueOf(words[2])));
            }
        }).keyBy("txId");

        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventDS.connect(receiptEvent);

        connect.process(new CoProcessFunction<OrderEvent, TxEvent, Tuple2<OrderEvent,TxEvent>>() {
            private HashMap<String,TxEvent> txMap = new HashMap<>();
            private HashMap<String,OrderEvent> orderMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent orderEvent, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
                if(txMap.get(orderEvent.getTxId()) == null){
                    orderMap.put(orderEvent.getTxId(),orderEvent);
                }else{
                    out.collect(Tuple2.of(orderEvent,txMap.get(orderEvent.getTxId())));
                }
            }

            @Override
            public void processElement2(TxEvent txEvent, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
                if(orderMap.get(txEvent.getTxId()) == null){
                    txMap.put(txEvent.getTxId(),txEvent);
                }else{
                    out.collect(Tuple2.of(orderMap.get(txEvent.getTxId()),txEvent));
                }
            }
        }).print();



        env.execute();
    }
}
