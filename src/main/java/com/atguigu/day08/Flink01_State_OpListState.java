package com.atguigu.day08;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
public class Flink01_State_OpListState {
    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.将数据压平
        SingleOutputStreamOperator<String> wordDS = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //4.使用状态实现map函数计算每个并行度中出现单词的次数
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToSumDS = wordDS.map(new MyRichMapFunc());

        //5.打印
        wordToSumDS.print();

        //6.启动
        env.execute();

    }

    public static class MyRichMapFunc extends RichMapFunction<String, Tuple2<String, Long>> implements CheckpointedFunction {

        //定义变量用来保存出现单词的总数
        private Long count;

        //定义状态
        private ListState<Long> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            count = 0L;
        }

        @Override
        public Tuple2<String, Long> map(String value) throws Exception {
            count++;
            return new Tuple2<>(value, count);
        }

        //从CK中获取状态恢复数据
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            //取出状态数据
            listState = context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Long>("list-state", Long.class));

            //恢复数据
            for (Long aLong : listState.get()) {
                count += aLong;
            }
        }

        //将状态保存至CK
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            listState.add(count);
        }

    }
}
