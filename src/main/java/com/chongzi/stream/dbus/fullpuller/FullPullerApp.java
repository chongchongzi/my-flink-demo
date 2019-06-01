package com.chongzi.stream.dbus.fullpuller;

import com.chongzi.stream.dbus.model.GlobalConfig;
import com.chongzi.stream.dbus.utils.JdbcUtil;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.split.NumericBetweenParametersProvider;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 全量拉取flink app(依赖flink-jdbc模块)
 */
@Slf4j
public class FullPullerApp {
    public static final boolean exploitParallelism = true;

    public static final String SPLIT_FIELD = "goodsId";

    public static final RowTypeInfo ROW_TYPE_INFO = new RowTypeInfo(
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.STRING_TYPE_INFO,
            BasicTypeInfo.BIG_DEC_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO,
            BasicTypeInfo.INT_TYPE_INFO);

    public static void main(String[] args) throws Exception{

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        Serializable[][] parameterValues=new NumericBetweenParametersProvider(10, 1, 100).getParameterValues();
//
//        for (Serializable[] serializables : parameterValues) {
//            System.out.println(serializables[0]+":"+serializables[1]);
//        }
        JDBCInputFormat.JDBCInputFormatBuilder inputBuilder = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(GlobalConfig.DRIVER_CLASS)
                .setDBUrl(GlobalConfig.DB_URL)
                .setUsername(GlobalConfig.USER_MAME)
                .setPassword(GlobalConfig.PASSWORD)
                .setQuery("select * from test.dajiangtai_goods")
                .setRowTypeInfo(ROW_TYPE_INFO);

        if (exploitParallelism) {
            //计算指定字段的min,max
            final int fetchSize = 2;

            Boundary boundary=boundaryQuery(SPLIT_FIELD);

            //use a "splittable" query to exploit parallelism
            inputBuilder = inputBuilder
                    .setQuery("select * from test.dajiangtai_goods WHERE "+SPLIT_FIELD+" BETWEEN ? AND ?")
                    //自动生成BETWEEN子句的参数
                    .setParametersProvider(new NumericBetweenParametersProvider(fetchSize, boundary.min, boundary.max));

        }
        DataSet<Row> source = env.createInput(inputBuilder.finish());

        //写到 Hbase大家自行完成
    }


    public static Boundary boundaryQuery(String splitField) throws Exception{
        String query = "select min("+splitField+"),max("+splitField+") from test.dajiangtai_goods";
        System.out.println(query);
        int min = 0;
        int max = 0;

        Connection conn=null;

        Statement stmt=null;

        ResultSet rs=null;

        try{
            conn= JdbcUtil.getConnection();

            stmt=conn.createStatement();

            rs=stmt.executeQuery(query);

            while (rs.next()) {
                min=rs.getInt(1);
                max=rs.getInt(2);

                log.info("min({}) : {}:max({}) : {}",splitField,min,splitField,max);

            }
        }finally {
            JdbcUtil.close(rs,stmt,conn);
        }
        return Boundary.of(min,max);
    }

    @Data
    @ToString
    public static class Boundary{
        private int min;
        private int max;

        public Boundary(int min, int max) {
            this.min = min;
            this.max = max;
        }

        public static Boundary of(int min, int max) {
            return new Boundary(min,max);
        }
    }

}
