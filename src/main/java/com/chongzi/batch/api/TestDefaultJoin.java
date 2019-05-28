package com.chongzi.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class TestDefaultJoin {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"ww"));
        data1.add(new Tuple2<>(4,"zl"));

        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"beijing"));
        data2.add(new Tuple2<>(2,"shanghai"));
        data2.add(new Tuple2<>(3,"guangzhou"));


        DataSource<Tuple2<Integer, String>> input1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> input2 = env.fromCollection(data2);

        DataSet<Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>>> joinedData=input1.join(input2)
                .where(0)
                .equalTo(0);

        joinedData.print();

    }
}
