package com.chongzi.dateset.api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * 以element为粒度，对element进行过滤操作。将满足过滤条件的element组成新的DataSet
 */
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> text = env.fromElements(2, 4, 7, 8, 9, 6);

        //2.对DataSet的元素进行过滤，筛选出偶数元素
        DataSet<Integer> text2 =text.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer e) throws Exception {
                return e%2==0;
            }
        });
        text2.print();

        //3.对DataSet的元素进行过滤，筛选出大于5的元素
        DataSet<Integer> text3 =text.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer e) throws Exception {
                return e>5;
            }
        });
        text3.print();

        DataSet<String> input = env.fromElements("zhangsan boy", "lisi is a girl so sex","wangwu boy");
        //4.过滤出包含'boy'字样的元素
        DataSet<String> input2 =input.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String e) throws Exception {
                return e.contains("boy");
            }
        });
        input2.print();

    }
}
