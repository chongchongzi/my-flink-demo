package com.chongzi.stream.userPurchaseBehaviorTracker.model;

import lombok.Data;
import lombok.ToString;

/**
 * @Description 商品实体类
 * @Author chongzi
 * @Date 2019/5/28 19:51
 * @Param 
 * @return 
 **/
@Data
@ToString
public class Product {
    private Integer productId;
    private double price;
    private Integer amount;
}
