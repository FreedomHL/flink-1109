package com.atguigu.day03;

import com.mysql.jdbc.Driver;
import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink16_Sink_JDBCSink {
    public static void main(String[] args) throws Exception {
        //TODO 使用官方提供的jdbc sink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> resultMap = ds.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] words = value.split(",");
                return new WaterSensor(words[0], Long.valueOf(words[1]), Double.valueOf(words[2]));
            }
        });

        String sql = "insert into sensor values (?,?,?) on duplicate key update ts=?,vc=?";

        resultMap.addSink(JdbcSink.sink(sql, new JdbcStatementBuilder<WaterSensor>() {
            @Override
            public void accept(PreparedStatement ps, WaterSensor waterSensor) throws SQLException {
                ps.setString(1,waterSensor.getId());
                ps.setLong(2,waterSensor.getTs());
                ps.setDouble(3,waterSensor.getVc());
                ps.setLong(4,waterSensor.getTs());
                ps.setDouble(5,waterSensor.getVc());
                ps.execute();
            }
        },
                JdbcExecutionOptions.builder().withBatchSize(1).build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));

        env.execute();
    }



}
