package com.atguigu.day09;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Flink03_CEP_LoginFail {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取文本数据
        DataStreamSource<String> readTextFile = env.readTextFile("input/LoginLog.csv");

        //3.转换为JavaBean
        SingleOutputStreamOperator<LoginEvent> loginEventDS = readTextFile.map(line -> {
            String[] words = line.split(",");
            return new LoginEvent(words[0], words[1],
                    words[2], Long.valueOf(words[3]));
        });


        //4.提取时间戳生成mk  wm延迟给大了 会怎么样
        SingleOutputStreamOperator<LoginEvent> loginEventSingleOutputStreamOperator = loginEventDS.assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        //5.分组
        KeyedStream<LoginEvent, String> loginEventStringKeyedStream = loginEventSingleOutputStreamOperator.keyBy(LoginEvent::getId);


        //6.定义模式序列 2s内连续2次失败
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getState());
                    }
                })
                /*.next("next").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getState());
                    }*/
                //})
            .times(5) //默认宽松近邻
                .consecutive() //严格近邻
                .within(Time.seconds(2));

        //7.将模式序列作用到流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStringKeyedStream, pattern);

        //8.提取事件
        SingleOutputStreamOperator<String> selectDS = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {

                List<LoginEvent> startList = map.get("start");
                //List<LoginEvent> nextList = map.get("next");

                LoginEvent startEvent = startList.get(0);
                LoginEvent startEvent1 = startList.get(4);
                //LoginEvent startEvent1 = startList.get(1);
                //LoginEvent startEvent2 = startList.get(2);

                //LoginEvent nextEvent = nextList.get(0);

                return startEvent.getId() + "在" + startEvent.getTs() + "到" + startEvent1.getTs() + "之间连续失败5次";
                //return startEvent.getId() + "在" + startEvent.getTs() + "到:"+startEvent1.getTs()+":+" + startEvent2.getTs() + "之间连续失败2次";
                //return startEvent.getId() + "在" + startEvent.getTs() + "到:" + "之间连续失败3次";
            }
        });

        //9.打印输出
        selectDS.print();
        //10.输出
        env.execute();

    }
}
