package com.chongzi.batch.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * 对DataSet中的元素进行去重
 */
public class DistinctDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1.创建一个 DataSet其元素为String类型
        DataSet<String> text = env.fromElements("lisi","zhangsan", "lisi","wangwu");
        //2.元素去重
        DataSet<String> text2 = text.distinct();
        text2.print();

        // 多项目的去重，不指定比较项目，默认是全部比较
        DataSet<Tuple3<Integer, String, Double>> data = env.fromElements(
                new Tuple3<>(2,"zhagnsan",1654.5),
                new Tuple3<>(3,"lisi",2347.8),
                new Tuple3<>(2,"zhagnsan",1654.5),
                new Tuple3<>(4,"wangwu",1478.9),
                new Tuple3<>(5,"zhaoliu",987.3),
                new Tuple3<>(2,"zhagnsan",1654.0)
        );
        DataSet<Tuple3<Integer, String, Double>> data2 = data.distinct();
        data2.print();


        //多项目的去重，指定比较项目
        DataSet<Tuple3<Integer, String, Double>> data3 = env.fromElements(
                new Tuple3<>(2,"zhagnsan",1654.5),
                new Tuple3<>(3,"lisi",2347.8),
                new Tuple3<>(2,"zhagnsan",1654.5),
                new Tuple3<>(4,"wangwu",1478.9),
                new Tuple3<>(5,"zhaoliu",987.3),
                new Tuple3<>(2,"zhagnsan",1654.0)
        );
        DataSet<Tuple3<Integer, String, Double>> data4 = data3.distinct(0,1);
        data4.print();


        //根据表达式进行去重
        DataSet<Integer> data5 = env.fromElements(3,-3,4,-4,6,-5,7);
    }
}
