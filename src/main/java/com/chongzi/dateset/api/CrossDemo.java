package com.chongzi.dateset.api;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * 交叉，拿第一个输入的每一个元素和第二个输入的每一个元素进行交叉操作
 */
public class CrossDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Integer, Integer>> coords1 = env.fromElements(
                new Tuple3<>(1,4,7),
                new Tuple3<>(2,5,8),
                new Tuple3<>(3,6,9)
        );
        DataSet<Tuple3<Integer, Integer, Integer>> coords2 = env.fromElements(
                new Tuple3<>(10,40,70),
                new Tuple3<>(20,50,80),
                new Tuple3<>(30,60,90)
        );
        // 2.交叉两个DataSet[Coord]
        CrossOperator.DefaultCross<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> text2 = coords1.cross(coords2);

        //3.显示结果
        text2.print();
    }
}
