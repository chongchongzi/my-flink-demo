package com.chongzi.batch.api;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 对每一组的元素对分组进行排序操作
 */
public class SortGroupDemo {

    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<String, Integer>> data = env.fromElements(
                new Tuple2("zhangsan", 1000), new Tuple2("lisi", 1001),
                new Tuple2("zhangsan", 3000), new Tuple2("lisi", 1002));

        //2.对DataSet的元素进行合并，这里是计算累加和
        DataSet<Tuple2<String, Integer>> text2 = data.groupBy(0).sortGroup(0,Order.ASCENDING).reduceGroup(
                new GroupReduceFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<String, Integer>> iterable,
                                       Collector<Tuple2<String, Integer>> collector) throws Exception {
                        int salary = 0;
                        String name = "";
                        Iterator<Tuple2<String, Integer>> itor = iterable.iterator();
                        //4.2统计每个人的工资总和
                        while (itor.hasNext()) {
                            Tuple2<String, Integer> t = itor.next();
                            name = t.f0;
                            salary = t.f1;
                            collector.collect(new Tuple2(name, salary));
                        }
                    }
                });
        text2.print();
    }
}
