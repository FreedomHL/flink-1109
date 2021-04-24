package com.atguigu.day12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FinkSQL01_TestHive {
    public static void main(String[] args) {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.创建HiveCataLog
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", "hiveconf");

        //3.将HiveCataLog注册进表执行环境
        tableEnv.registerCatalog("hive",hiveCatalog);

        //4.使用HiveCatalog，并指定库
        tableEnv.useCatalog("hive");
        tableEnv.useDatabase("default");

        tableEnv.sqlQuery("select * from emp")
                .execute()
                .print();

    }
}
