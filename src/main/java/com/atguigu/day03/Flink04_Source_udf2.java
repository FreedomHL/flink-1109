package com.atguigu.day03;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Flink04_Source_udf2 {
    public static void main(String[] args) throws Exception {
        //TODO 自定义输入源再来一次

        //1、创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2、添加自定义输入源
        DataStreamSource<WaterSensor> source = env.addSource(new MySource("hadoop102", 9999));
        source.print();

        //执行
        env.execute();

    }
    //TODO 接收从9999端口输入的watersensor格式的数据
    public static class MySource implements SourceFunction<WaterSensor>{
        //标识符，控制开关
        boolean running = true;

        //定义属性，接收host和port
        private String host;
        private int port;

        Socket socket = null;
        BufferedReader reader = null;

        public MySource(){}
        public MySource(String host,int port){
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            //开启输入流接收控制台输入的数据
            socket = new Socket(host,port);
            //字节流转换为字符流
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            String line = reader.readLine();

            while(running && line != null){
                String[] split = line.split(",");
                ctx.collect(new WaterSensor(split[0],Long.valueOf(split[1]),Double.valueOf(split[2])));
                line = reader.readLine();
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
