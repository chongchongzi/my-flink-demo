package com.chongzi.batch.api;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class TestPartition {
    public static void main(String[] args) throws Exception {
        //获取运行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(1,"hello1"));
        data.add(new Tuple2<>(2,"hello2"));
        data.add(new Tuple2<>(2,"hello3"));
        data.add(new Tuple2<>(3,"hello4"));
        data.add(new Tuple2<>(3,"hello5"));
        data.add(new Tuple2<>(3,"hello6"));
        data.add(new Tuple2<>(4,"hello7"));
        data.add(new Tuple2<>(4,"hello8"));
        data.add(new Tuple2<>(4,"hello9"));
        data.add(new Tuple2<>(4,"hello10"));
        data.add(new Tuple2<>(5,"hello11"));
        data.add(new Tuple2<>(5,"hello12"));
        data.add(new Tuple2<>(5,"hello13"));
        data.add(new Tuple2<>(5,"hello14"));
        data.add(new Tuple2<>(5,"hello15"));
        data.add(new Tuple2<>(6,"hello16"));
        data.add(new Tuple2<>(6,"hello17"));
        data.add(new Tuple2<>(6,"hello18"));
        data.add(new Tuple2<>(6,"hello19"));
        data.add(new Tuple2<>(6,"hello20"));
        data.add(new Tuple2<>(6,"hello21"));


        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        text.partitionByHash(0).mapPartition(new PartitionMapper()).print();

        //text.partitionByRange(0).mapPartition(new PartitionMapper()).print();

    }

    public static class PartitionMapper extends RichMapPartitionFunction<Tuple2<Integer,String>, Tuple2<Integer,String>> {

        @Override
        public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
            for(Tuple2<Integer,String> item:values){
                System.out.println("当前subtaskIndex："+getRuntimeContext().getIndexOfThisSubtask()+","+item);

            }
        }
    }
}
