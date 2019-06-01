package com.chongzi.stream.dbus.simulator;

import com.chongzi.stream.dbus.model.GlobalConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.sql.Types;

public class GoodsSimulator {

    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple5<Integer,String,BigDecimal,Integer,Integer>> inputs=env.fromElements(
                Tuple5.of(1,"Apple iPhone X (A1865)",BigDecimal.valueOf(6319.00),10000,34564),
                Tuple5.of(2,"vivo iQOO",BigDecimal.valueOf(3298.00),20000,3433),
                Tuple5.of(3,"AppleMQHV2CH/A",BigDecimal.valueOf(2749.00),2000,342221),
                Tuple5.of(4,"AppleApple Watch",BigDecimal.valueOf(2099.00),5587,22111),
                Tuple5.of(5,"小米8",BigDecimal.valueOf(2299.00),10000,1298)
        );

        DataSet<Row> goods=inputs.map(new MapFunction<Tuple5<Integer,String, BigDecimal, Integer, Integer>, Row>() {
            @Override
            public Row map(Tuple5<Integer,String, BigDecimal, Integer, Integer> value) throws Exception {
                return Row.of(value.f0,value.f1,value.f2,value.f3,value.f4);
            }
        });

        goods.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(GlobalConfig.DRIVER_CLASS)
                .setDBUrl(GlobalConfig.DB_URL)
                .setUsername(GlobalConfig.USER_MAME)
                .setPassword(GlobalConfig.PASSWORD)
                .setBatchInterval(GlobalConfig.BATCH_SIZE)
                .setQuery("insert into dajiangtai_goods (goodsId, goodsName, sellingPrice, goodsStock, appraiseNum) values (?,?,?,?,?)")
                .setSqlTypes(new int[]{Types.INTEGER,Types.VARCHAR, Types.DECIMAL, Types.INTEGER, Types.INTEGER})
                .finish());

        env.execute();

    }

}
