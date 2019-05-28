package com.chongzi.stream.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @Description 配置
 * @Author chongzi
 * @Date 2019/5/28 19:51
 * @Param 
 * @return 
 **/
@Data
@ToString
public class Config implements Serializable {
    private static final long serialVersionUID = 4416826133199103653L;

    //{"channel":"APP","registerDate":"2018-01-01","historyPurchaseTimes":0,"maxPurchasePathLength":3}

    /**
     * 渠道
     */
    private String channel;
    /**
     * 规则创建时间
     */
    private String registerDate;
    /**
     * 历史购买时间
     */
    private Integer historyPurchaseTimes;
    /**
     * 最大购买路径
     */
    private Integer maxPurchasePathLength;

    public Config() {
    }

    public Config(String channel, String registerDate, Integer historyPurchaseTimes, Integer maxPurchasePathLength) {
        this.channel = channel;
        this.registerDate = registerDate;
        this.historyPurchaseTimes = historyPurchaseTimes;
        this.maxPurchasePathLength = maxPurchasePathLength;
    }
}
