package com.chongzi.batch.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 以element为粒度，对element进行1：1的转化
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        // 1.设置运行环境，准备运行的数据
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.fromElements("flink vs spark", "buffer vs  shuffle");

        // 2.以element为粒度，将element进行map操作，转化为大写并添加后缀字符串"--##bigdata##"
        DataSet<String> text2 = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.toUpperCase() + "--##bigdata##";
            }
        });
        text2.print();

        // 4.以element为粒度，将element进行map操作，转化为大写并,并计算line的长度。
        DataSet< Tuple2<String, Integer>> text3= text.map(
                new MapFunction<String, Tuple2<String,Integer> >() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        //转化为大写并,并计算矩阵的长度。
                        return new Tuple2<String, Integer>(s.toUpperCase(),s.length());
                    }
                });
        text3.print();

        // 4.以element为粒度，将element进行map操作，转化为大写并,并计算line的长度。
        //4.1定义class
        class Wc{
            private String line;
            private int lineLength;
            public Wc(String line, int lineLength) {
                this.line = line;
                this.lineLength = lineLength;
            }

            @Override
            public String toString() {
                return "Wc{" + "line='" + line + '\'' + ", lineLength='" + lineLength + '\'' + '}';
            }
        }
        //4.2转化成class类型
        DataSet<Wc> text4= text.map(new MapFunction<String, Wc>() {
            @Override
            public Wc map(String s) throws Exception {
                return new Wc(s.toUpperCase(),s.length());
            }
        });
        text4.print();

        //1.创建一个DataSet其元素为Int类型
        DataSet<Integer> input = env.fromElements(23, 67, 18, 29, 32, 56, 4, 27);

        // 2.将DataSet中的每个元素乘以2
        DataSet<Integer> input2 = input.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer s) throws Exception {
                return s*2;
            }
        });
        input2.print();

        //1..创建一个DataSet<Tuple2<Integer,Integer>>
        DataSet<Tuple2<Integer,Integer>> intPairs = env.fromElements(new Tuple2(18,4),new Tuple2(19,5),new Tuple2(23,6),new Tuple2(38,3));

        // 2.键值对的key+value之和生成新的dataset
        DataSet<Integer> intPairs2 = intPairs.map(new MapFunction<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer map(Tuple2<Integer, Integer> t) throws Exception {
                return t.f0+t.f1;
            }
        });
        intPairs2.print();
    }
}
