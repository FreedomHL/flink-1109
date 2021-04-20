package com.atguigu.day04;

import com.atguigu.day04.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class Flink06_Practice_MarketByChannel {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<MarketingUserBehavior> originDS = env.addSource(new AppMarketingDataSource());

        //originDS.print();

        //按渠道和行为分组
        KeyedStream<MarketingUserBehavior, Tuple2<String, String>> keyedStream = originDS.keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getChannel(), value.getBehavior());
            }
        });

        //计算总和
        keyedStream.process(new KeyedProcessFunction<Tuple2<String, String>, MarketingUserBehavior, Tuple2<Tuple2<String,String>,Long>>() {
            private HashMap<String,Long> map = new HashMap<>();
            //一个并行度下会有不同的渠道和行为
            @Override
            public void processElement(MarketingUserBehavior value, Context ctx, Collector<Tuple2<Tuple2<String, String>, Long>> out) throws Exception {
                String str = value.getChannel() + value.getBehavior();

                map.put(str,map.getOrDefault(str,0L) + 1);

                //out.collect(Tuple2.of(Tuple2.of(value.getChannel(),value.getBehavior()),map.get(str)));
                out.collect(Tuple2.of(ctx.getCurrentKey(),map.get(str)));
            }
        }).print();
        
        //flatMap.keyBy(0).sum(1).print();


        env.execute();
    }
    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceFunction.SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(200);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
