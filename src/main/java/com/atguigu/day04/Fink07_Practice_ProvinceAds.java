package com.atguigu.day04;

import com.atguigu.day04.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Fink07_Practice_ProvinceAds {
    public static void main(String[] args) throws Exception {
        //TODO 各省份页面广告点击量实时统计
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.readTextFile("input/AdClickLog.csv");

        SingleOutputStreamOperator<AdsClickLog> mapDs = ds.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] words = value.split(",");
                return new AdsClickLog(
                        Long.valueOf(words[0]),
                        Long.valueOf(words[1]),
                        words[2], words[3],
                        Long.valueOf(words[4]));
            }
        });

                                                                                 //输入的字段 和 以哪个字段分组
        KeyedStream<AdsClickLog, Tuple2<String, Long>> keyedStream = mapDs.keyBy(new KeySelector<AdsClickLog, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(AdsClickLog value) throws Exception {
                return Tuple2.of(value.getProvince(), value.getAdId());
            }
        });
        //key类型，输入数据，输出数据
        keyedStream.process(new KeyedProcessFunction<Tuple2<String, Long>, AdsClickLog, Tuple2<Tuple2<String,Long>,Long>>() {
            //同一个并行度下，省份和广告id可能会不一样
            HashMap<String,Long> map = new HashMap<>();
            @Override
            public void processElement(AdsClickLog value, Context ctx, Collector<Tuple2<Tuple2<String, Long>, Long>> out) throws Exception {
                String str = value.getProvince() + value.getAdId();
                Long cnt = map.getOrDefault(str,0L);
                cnt++;
                map.put(str,cnt);
                out.collect(Tuple2.of(ctx.getCurrentKey(),cnt));
            }
        }).print();


        env.execute();
    }
}
