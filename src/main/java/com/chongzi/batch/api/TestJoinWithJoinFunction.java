package com.chongzi.batch.api;

import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class TestJoinWithJoinFunction {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"ww"));
        data1.add(new Tuple2<>(4,"zl"));
        data1.add(new Tuple2<>(5,"mq"));

        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"beijing"));
        data2.add(new Tuple2<>(2,"shanghai"));
        data2.add(new Tuple2<>(3,"guangzhou"));


        DataSource<Tuple2<Integer, String>> input1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> input2 = env.fromCollection(data2);

        DataSet<UserInfo> joinedData=input1.join(input2)
                .where(0)
                .equalTo(0)
                .with(new UserInfoJoinFun());

        joinedData.print();
    }

    public static class UserInfoJoinFun implements JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>,UserInfo> {

        @Override
        public UserInfo join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
            return UserInfo.of(first.f0,first.f1,second.f1);
        }
    }

    @Data
    @ToString
    public static class UserInfo{
        private Integer userId;
        private String userName;
        private String address;

        public UserInfo() {
        }

        public UserInfo(Integer userId, String userName, String address) {
            this.userId = userId;
            this.userName = userName;
            this.address = address;
        }

        public static UserInfo of(Integer userId, String userName, String address){
            return new UserInfo(userId,userName,address);
        }
    }
}
