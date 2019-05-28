package com.chongzi.batch.hbase.demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * @Author: 🐟lifei🐟
 * @Date: 2019/2/23 下午5:14
 */
public class TestWriteToHBase {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple4<String, String, Integer, String>> users=env.fromElements(
                Tuple4.of("1000","zhansgan",30,"beijing"),
                Tuple4.of("1001","lisi",23,"guangdong"),
                Tuple4.of("1002","wangwu",45,"hubei"),
                Tuple4.of("1003","zhanliu",23,"beijing"),
                Tuple4.of("1004","lilei",56,"henan"),
                Tuple4.of("1005","maxiaoshuai",34,"xizang"),
                Tuple4.of("1006","liudehua",26,"fujian"),
                Tuple4.of("1007","jiangxiaohan",18,"hubei"),
                Tuple4.of("1008","qianjin",29,"shanxi"),
                Tuple4.of("1009","zhujie",37,"shandong"),
                Tuple4.of("1010","taobinzhe",19,"guangxi"),
                Tuple4.of("1011","wuqixian",20,"hainan")
                );

//        users.print();

        //生成HBase输出数据
        DataSet<Tuple2<Text, Mutation>> result = convertResultToMutation(users);

        //4.输出到HBase(直接使用hadoop的OutputFormat)

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "slave01,slave02,slave03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set(TableOutputFormat.OUTPUT_TABLE, "learing_flink:users");
        conf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp");

        Job job = Job.getInstance(conf);

        result.output(new HadoopOutputFormat<Text, Mutation>(new TableOutputFormat<Text>(), job));

        env.execute("TestWriteToHBase");
    }

    public static DataSet<Tuple2<Text, Mutation>> convertResultToMutation(DataSet<Tuple4<String, String, Integer, String>> users) {
        return users.map(new RichMapFunction<Tuple4<String, String, Integer, String>, Tuple2<Text, Mutation>>() {

            private transient Tuple2<Text, Mutation> resultTp;

            private byte[] cf ="F".getBytes(ConfigConstants.DEFAULT_CHARSET);

            @Override public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                resultTp = new Tuple2<>();
            }

            @Override
            public Tuple2<Text, Mutation> map(Tuple4<String, String, Integer, String> user) throws Exception {
                resultTp.f0 = new Text(user.f0);
                Put put = new Put(user.f0.getBytes(ConfigConstants.DEFAULT_CHARSET));

                if (null != user.f1) {
                    put.addColumn(cf, Bytes.toBytes("name"), Bytes.toBytes(user.f1));
                }

                //记得一定要先toString
                put.addColumn(cf, Bytes.toBytes("age"), Bytes.toBytes(user.f2.toString()));

                if (null != user.f3) {
                    put.addColumn(cf, Bytes.toBytes("address"), Bytes.toBytes(user.f3));
                }

                resultTp.f1 = put;
                return resultTp;
            }
        });
    }
}
