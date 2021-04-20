package com.atguigu.day03;

import com.atguigu.day03.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;


public class Flink04_Source_udf {
    public static void main(String[] args) throws Exception {

        //1、创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> source = env.addSource(new MySource("hadoop102", 9999));

        source.print();

        env.execute();
    }
    //自定义从端口读取数据的source
    public static class MySource implements SourceFunction<WaterSensor>{
        //定义属性，主机和端口号
        private String host = null;
        private int port = 0;
        boolean running = true;
        Socket socket = null;
        BufferedReader reader = null;
        public MySource(){

        }
        public MySource(String host,int port){
            this.host = host;
            this.port = port;
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            //创建输入流
            socket = new Socket(host,port);
            //字节流-》字符流
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),StandardCharsets.UTF_8));
            String line = reader.readLine();

            while(running && line != null){
                String[] words = line.split(",");
                WaterSensor waterSensor = new WaterSensor(words[0], Long.valueOf(words[1]),Double.valueOf(words[2]));
                ctx.collect(waterSensor);
                line = reader.readLine();
            }
        }

        @Override
        public void cancel() {
            running = false;
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
