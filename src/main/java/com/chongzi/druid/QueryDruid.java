package com.chongzi.druid;

import com.fasterxml.jackson.databind.ObjectMapper;
import in.zapr.druid.druidry.Interval;
import in.zapr.druid.druidry.aggregator.CountAggregator;
import in.zapr.druid.druidry.aggregator.DoubleSumAggregator;
import in.zapr.druid.druidry.aggregator.DruidAggregator;
import in.zapr.druid.druidry.aggregator.LongSumAggregator;
import in.zapr.druid.druidry.client.DruidClient;
import in.zapr.druid.druidry.client.DruidConfiguration;
import in.zapr.druid.druidry.client.DruidJerseyClient;
import in.zapr.druid.druidry.dimension.DruidDimension;
import in.zapr.druid.druidry.dimension.SimpleDimension;
import in.zapr.druid.druidry.granularity.Granularity;
import in.zapr.druid.druidry.granularity.PredefinedGranularity;
import in.zapr.druid.druidry.granularity.SimpleGranularity;
import in.zapr.druid.druidry.query.aggregation.DruidTimeSeriesQuery;
import in.zapr.druid.druidry.query.aggregation.DruidTopNQuery;
import in.zapr.druid.druidry.topNMetric.SimpleMetric;
import in.zapr.druid.druidry.topNMetric.TopNMetric;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Arrays;
import java.util.Collections;

/**
 * @Description 获取一个月的数据，参考https://github.com/zapr-oss/druidry
 * @Author chongzi
 * @Date 2018/9/29 18:22
 **/
public class QueryDruid {

    public static void main(String[] args) throws Exception {
        DruidConfiguration config =  DruidConfiguration
                .builder()
                .host("127.0.0.1")
                .endpoint("druid/v2/")
                .build();
        DruidClient client = new DruidJerseyClient(config);
        client.connect();
        DruidAggregator createOrderPrice = new DoubleSumAggregator("createOrderPrice",
                "createOrderPrice");
        DruidAggregator createOrderQuantity = new LongSumAggregator("createOrderQuantity",
                "createOrderQuantity");
        DruidAggregator gmv = new DoubleSumAggregator("gmv",
                "gmv");
        DruidAggregator purchaseOrderPrice = new DoubleSumAggregator("purchaseOrderPrice",
                "purchaseOrderPrice");
        DruidAggregator purchaseOrderQuantity = new LongSumAggregator("purchaseOrderQuantity",
                "purchaseOrderQuantity");
        DruidAggregator purchaseOrderAmount = new DoubleSumAggregator("purchaseOrderAmount",
                "purchaseOrderAmount");
        DruidAggregator skuExpCnt = new LongSumAggregator("skuExpCnt",
                "skuExpCnt");
        DruidAggregator skuHitCnt = new LongSumAggregator("skuHitCnt",
                "skuHitCnt");
        DruidAggregator skuCartCnt = new LongSumAggregator("skuCartCnt",
                "skuCartCnt");
        DruidAggregator skuMarkedCnt = new LongSumAggregator("skuMarkedCnt",
                "skuMarkedCnt");
        DruidAggregator createOrderCnt = new LongSumAggregator("createOrderCnt",
                "createOrderCnt");
        DruidAggregator purchaseOrderCnt = new LongSumAggregator("purchaseOrderCnt",
                "purchaseOrderCnt");
        DateTime startTime = new DateTime(2018, 10, 1, 0, 0, 0, DateTimeZone.UTC);
        DateTime endTime = new DateTime(2018, 10, 31, 0, 0, 0, DateTimeZone.UTC);
        Interval interval = new Interval(startTime, endTime);

        Granularity granularity = new SimpleGranularity(PredefinedGranularity.ALL);
        DruidTimeSeriesQuery query = DruidTimeSeriesQuery.builder()
                .dataSource("mycount")
                .granularity(granularity)
                .aggregators(Arrays.asList(createOrderPrice,createOrderQuantity,gmv,purchaseOrderPrice,purchaseOrderQuantity,purchaseOrderAmount,skuExpCnt,skuHitCnt,skuCartCnt,skuMarkedCnt,createOrderCnt,purchaseOrderCnt))
                .intervals(Collections.singletonList(interval))
                .build();

        ObjectMapper mapper = new ObjectMapper();
        String requiredJson = mapper.writeValueAsString(query);
        System.out.println(requiredJson);
        String responses = client.query(query);
        System.out.println(responses);
        client.close();
    }
}
