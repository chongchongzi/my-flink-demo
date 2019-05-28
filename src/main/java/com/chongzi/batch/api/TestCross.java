package com.chongzi.batch.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;

public class TestCross {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id，用户姓名>
        ArrayList<String> data1 = new ArrayList<>();
        data1.add("zs");
        data1.add("ww");
        //tuple2<用户id，用户所在城市>
        ArrayList<Integer> data2 = new ArrayList<>();
        data2.add(1);
        data2.add(2);
        DataSource<String> input1 = env.fromCollection(data1);
        DataSource<Integer> input2 = env.fromCollection(data2);
        CrossOperator.DefaultCross<String, Integer> cross = input1.cross(input2);
        cross.print();
    }
}
