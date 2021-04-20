package com.atguigu.day03;

import com.alibaba.fastjson.JSON;
import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class Flink14_Sink_redis {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 9999);

        //输入数据转换为java对象
        SingleOutputStreamOperator<WaterSensor> maprResult = ds.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });

        //创建fink-redis连接器
        FlinkJedisPoolConfig jedisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        //添加redisSink,写入到redis
        maprResult.addSink(new RedisSink<>(jedisConfig,new MyRedisMapper()));

        env.execute();


    }
                                                    //要处理的元素的类型
    public static class MyRedisMapper implements RedisMapper<WaterSensor>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            //HSET就是map，"sensor"类似于map的名字
            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
        }

        //key
        @Override
        public String getKeyFromData(WaterSensor data) {
            return data.getId();
        }

        //value
        @Override
        public String getValueFromData(WaterSensor data) {
            return JSON.toJSONString(data);
        }
    }
}
