package com.atguigu.day10;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL02_DataStreamToTable_Agg {
    public static void main(String[] args) throws Exception {
        //TODO 将流转化为表  Restract流
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据，并转化为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });

        //3.通过执行环境，创建流-》表的环境
        StreamTableEnvironment envTable = StreamTableEnvironment.create(env);

        //4.将流转化为动态表
        Table table = envTable.fromDataStream(waterSensorDS);

        //5.查询
        Table resultTable = table
                .groupBy($("id"))
                .aggregate($("id").count().as("cnt"))
                .select($("id"), $("cnt"));
        //将表转化为流
        DataStream<Tuple2<Boolean, Row>> dataStream = envTable.toRetractStream(resultTable, Row.class);

        //6.打印
        dataStream.print();

        //7.执行
        env.execute();
    }
}
