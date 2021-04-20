package com.atguigu.day03;

import com.alibaba.fastjson.JSON;
import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

public class Flink15_Sink_es {
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

        List<HttpHost> list = new ArrayList<>();
        list.add(new HttpHost("hadoop102",9200));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(list, new MyElasticsearch());

        //批量提交参数
        waterSensorBuilder.setBulkFlushMaxActions(1);

        ElasticsearchSink<WaterSensor> build = waterSensorBuilder.build();
        mapResult.addSink(build);


        env.execute();
    }
    public static class MyElasticsearch implements ElasticsearchSinkFunction<WaterSensor>{

        @Override
        public void process(WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

            //创建index请求
            IndexRequest source = Requests.indexRequest()
                    .index("sensor1")
                    .type("_doc")
                    .id(waterSensor.getId())
                    .source(JSON.toJSONString(waterSensor), XContentType.JSON);
            requestIndexer.add(source);
        }
    }
}
