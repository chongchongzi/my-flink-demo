package com.chongzi.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class TestDataSetUtils {

    public static void main(String[] args) throws Exception{

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Long,String,Integer>> inputs=env.fromElements(
                Tuple3.of(1L,"zhangsan",28),
                Tuple3.of(3L,"lisi",34),
                Tuple3.of(3L,"wangwu",23),
                Tuple3.of(3L,"zhaoliu",34),
                Tuple3.of(3L,"maqi",25)
        ).setParallelism(3);

//        inputs.mapPartition(new RichMapPartitionFunction<Tuple3<Long,String,Integer>, Object>() {
//
//            @Override
//            public void mapPartition(Iterable<Tuple3<Long, String, Integer>> values, Collector<Object> out) throws Exception {
//                for(Tuple3<Long, String, Integer> item:values){
//                    System.out.println("当前subtaskIndex："+getRuntimeContext().getIndexOfThisSubtask()+","+item);
//                }
//            }
//        }).print();

//        DataSetUtils.countElementsPerPartition(inputs).print();

        //生成连续的index(先count，在分配)
//        DataSetUtils.zipWithIndex(inputs).print();

        //生成唯一索引(不连续，流水线作业)
//        DataSetUtils.zipWithUniqueId(inputs).print();

        //采样类操作(输入Dataset,是否可以重复,每个元素被选中的概率-不能重复时[0,1]，可以重复时[0, ∞))
//        DataSetUtils.sample(inputs,false,0.5).print();

        //指定随机数生成器的种子，种子不变，生成的随机数不变
//        DataSetUtils.sample(inputs,false,0.3,2).print();

//        DataSetUtils.sampleWithSize(inputs,false,3).print();

//        DataSetUtils.sampleWithSize(inputs,false,3,3).print();

        //对每个列进行统计
//        System.out.println(DataSetUtils.summarize(inputs));

    }
}
