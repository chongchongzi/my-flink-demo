package com.chongzi.batch.api;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class TestSortPartition {
    public static void main(String[] args) throws Exception{
        //获取运行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2,"zs"));
        data.add(new Tuple2<>(4,"ls"));
        data.add(new Tuple2<>(3,"ww"));
        data.add(new Tuple2<>(1,"xw"));
        data.add(new Tuple2<>(1,"aw"));
        data.add(new Tuple2<>(1,"mw"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data).setParallelism(2);
        //获取前3条数据，按照数据插入的顺序
//        text.first(3).print();

        //根据数据中的第一列进行分组，获取每组的前2个元素
//        text.groupBy(0).first(2).print();

        //根据数据中的第一列分组，再根据第二列进行组内排序[升序]，获取每组的前2个元素
//        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();

        //不分组，分区内针对第一个元素升序，第二个元素倒序
        text.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).mapPartition(new PartitionMapper()).print();
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
