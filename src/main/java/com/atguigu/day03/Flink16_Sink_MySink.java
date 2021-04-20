package com.atguigu.day03;

import com.alibaba.fastjson.JSON;
import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public class Flink16_Sink_MySink {
    //TODO 自定义jdbc sink
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //无界流
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 9999);
        //有界流读完数据后，会自动进行收尾工作，进行文件刷写
        //DataStreamSource<String> ds = env.readTextFile("input/sensor.txt");

        SingleOutputStreamOperator<WaterSensor> mapResult = ds.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });

        mapResult.addSink(new MyDefineSink());


        env.execute();
    }
    public static class MyDefineSink extends RichSinkFunction<WaterSensor> {
        Connection conn = null;
        PreparedStatement ps = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://hadoop102/test?useSSL=false","root","123456");

            ps = conn.prepareStatement("insert into sensor values (?,?,?) on duplicate key update ts=?,vc=?");


        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            ps.setString(1,value.getId());
            ps.setLong(2,value.getTs());
            ps.setDouble(3,value.getVc());
            ps.setLong(4,value.getTs());
            ps.setDouble(5,value.getVc());
            ps.execute();
        }

        @Override
        public void close() throws Exception {
            ps.close();
            conn.close();
        }
    }


}
