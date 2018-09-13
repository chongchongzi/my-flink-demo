package com.chongzi.dateset.api;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作
 */
public class GroupByDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1.创建DataSet
        DataSet<Tuple2<String, Integer>> input = env.fromElements(new Tuple2("zhangasn",16),
                new Tuple2("zhangasn",17),
                new Tuple2("zhangasn",18),
                new Tuple2("lisi",16),
                new Tuple2("lisi",17),
                new Tuple2("wangwu",20));
        //2.使用自定义的reduce方法,使用key-expressions
        DataSet<Tuple2<String, Integer>> input2  = input.groupBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2(t1.f0, t1.f1+t2.f1);
            }
        });
        input2.print();

        //3.使用自定义的reduce方法,使用key-selector
        DataSet<Tuple2<String, Integer>> input3  = input.groupBy("f0").reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                return new Tuple2(stringIntegerTuple2.f0, stringIntegerTuple2.f1+t1.f1);
            }
        });
        input3.print();


        DataSet<Tuple3<String,String, Integer>> tuples = env.fromElements(new Tuple3("lisi","shandong",2400),
                new Tuple3("zhangsan","henan",2600),
                new Tuple3("lisi","shandong",2700),
                new Tuple3("lisi","guangdong",2800));

        //4.使用自定义的reduce方法,使用多个Case Class Fields name
        DataSet<Tuple3<String,String, Integer>> input4  = tuples.groupBy("f0","f1").reduce(new ReduceFunction<Tuple3<String,String, Integer>>() {
            @Override
            public Tuple3<String,String, Integer> reduce(Tuple3<String,String, Integer> t1, Tuple3<String,String, Integer> t2) throws Exception {
                return new Tuple3(t1.f0+"-"+t2.f0,t1.f1+"-"+t2.f1,t1.f2+t2.f2);
            }
        });
        input4.print();

        //5.使用自定义的reduce方法,使用多个Case Class Fields index
        DataSet<Tuple3<String,String, Integer>> input5  = tuples.groupBy(0,1).reduce(new ReduceFunction<Tuple3<String,String, Integer>>() {
            @Override
            public Tuple3<String,String, Integer> reduce(Tuple3<String,String, Integer> t1, Tuple3<String,String, Integer> t2) throws Exception {
                return new Tuple3(t1.f0+"-"+t2.f0,t1.f1+"-"+t2.f1,t1.f2+t2.f2);
            }
        });
        input5.print();
    }
}
