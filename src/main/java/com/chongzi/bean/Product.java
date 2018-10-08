package com.chongzi.bean;

/**
 * @Description 商品维度实体类
 * @Author chongzi
 * @Date 2018/10/7 17:05
 **/
public class Product {
    /**
     * app的名称
     */
    private String app_name;
    /**
     * 终端
     */
    private String platform;
    /**
     * 事件时间
     */
    private String event_time;
    /**
     * 事件名称
     */
    private String event_name;
    /**
     * sku
     */
    private String sku;
    /**
     * 数量
     */
    private Integer num;

    public Product() {
    }

    public Product(String app_name, String platform, String event_time, String event_name, String sku, Integer num) {
        this.app_name = app_name;
        this.platform = platform;
        this.event_time = event_time;
        this.event_name = event_name;
        this.sku = sku;
        this.num = num;
    }

    public String getApp_name() {
        return app_name;
    }

    public void setApp_name(String app_name) {
        this.app_name = app_name;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getEvent_time() {
        return event_time;
    }

    public void setEvent_time(String event_time) {
        this.event_time = event_time;
    }

    public String getEvent_name() {
        return event_name;
    }

    public void setEvent_name(String event_name) {
        this.event_name = event_name;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

}
