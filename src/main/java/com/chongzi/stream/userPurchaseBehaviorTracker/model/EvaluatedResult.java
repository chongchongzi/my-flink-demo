package com.chongzi.stream.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

import java.util.Map;

@Data
@ToString
public class EvaluatedResult {
    //{"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","purchasePathLength":9,"eventTypeCounts":{"ADD_TO_CART":1,"PURCHASE":1,"VIEW_PRODUCT":7}}
    /**
     * 用户
     */
    private String userId;
    /**
     * 渠道
     */
    private String channel;
    /**
     * 购买路径
     */
    private Integer purchasePathLength;
    /**
     * 购买行为
     */
    private Map<String,Integer> eventTypeCounts;
}
