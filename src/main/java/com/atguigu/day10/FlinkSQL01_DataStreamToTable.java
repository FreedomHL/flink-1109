package com.atguigu.day10;


import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_DataStreamToTable {
    public static void main(String[] args) throws Exception {
        //TODO 将流转化为动态表 Append-only 流
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据并将数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });

        //3.将流转换为动态表
        //3.1 获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //3.2 转换
        Table table = tableEnv.fromDataStream(waterSensorDS);

        //4.使用TableAPI执行查询
//        Table resultTable = sensorTable
//                .where("id = 'ws_001'")
//                .select("id,ts,vc");
        Table resultTable = table.where($("id").isEqual("ws_001"))
                .select($("id"));


        //5.将动态表转换为流
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(resultTable, Row.class);
        //6.打印输出
        rowDataStream.print();
        //7.启动
        env.execute();
    }
}
