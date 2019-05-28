package com.chongzi.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class TestDistinct {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long,String,Integer>> inputs=env.fromElements(
                Tuple3.of(1L,"zhangsan",28),
                Tuple3.of(3L,"lisi",34),
                Tuple3.of(3L,"wangwu",23),
                Tuple3.of(3L,"zhaoliu",34),
                Tuple3.of(3L,"maqi",25)
        );

        inputs.distinct(0).print();
    }
}
