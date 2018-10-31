package com.chongzi.bean;

/**
 * @Description 商品明细维度实体类
 * @Author chongzi
 * @Date 2018/10/7 17:05
 **/
public class ProductDetail {
    /**
     * app的名称
     */
    private String site;
    /**
     * 终端
     */
    private String divice;
    /**
     * 事件名称
     */
    private String eventName;
    /**
     * sku
     */
    private String sku;
    /**
     * 事件时间
     */
    private String eventTime;
    /**
     * 事件年份
     */
    private Integer eventYear;
    /**
     * 事件月份
     */
    private Integer eventMonth;
    /**
     * 事件日期
     */
    private String eventDate;
    /**
     * 创建订单价格
     */
    private Double createOrderPrice;
    /**
     * 创建订单数量
     */
    private Integer createOrderQuantity;

    /**
     * 创建订单总额
     */
    private Double gmv;
    /**
     * 支付订单价格
     */
    private Double purchaseOrderPrice;
    /**
     * 支付订单数量
     */
    private Integer purchaseOrderQuantity;

    /**
     * 支付订单总额
     */
    private Double purchaseOrderAmount;
    /**
     * 商品被曝光数量
     */
    private Integer skuExpCnt;
    /**
     * 商品被点击数量
     */
    private Integer skuHitCnt;
    /**
     * 商品被加购物车数量
     */
    private Integer skuCartCnt;
    /**
     * 商品被加收藏数量
     */
    private Integer skuMarkedCnt;
    /**
     * 创建订单总数
     */
    private Integer createOrderCnt;
    /**
     * 购买订单总数
     */
    private Integer purchaseOrderCnt;

    public ProductDetail() {
    }

    public ProductDetail(String site, String divice, String eventName, String sku, String eventTime, Integer eventYear, Integer eventMonth, String eventDate, Double createOrderPrice, Integer createOrderQuantity, Double gmv, Double purchaseOrderPrice, Integer purchaseOrderQuantity, Double purchaseOrderAmount, Integer skuExpCnt, Integer skuHitCnt, Integer skuCartCnt, Integer skuMarkedCnt, Integer createOrderCnt, Integer purchaseOrderCnt) {
        this.site = site;
        this.divice = divice;
        this.eventName = eventName;
        this.sku = sku;
        this.eventTime = eventTime;
        this.eventYear = eventYear;
        this.eventMonth = eventMonth;
        this.eventDate = eventDate;
        this.createOrderPrice = createOrderPrice;
        this.createOrderQuantity = createOrderQuantity;
        this.gmv = gmv;
        this.purchaseOrderPrice = purchaseOrderPrice;
        this.purchaseOrderQuantity = purchaseOrderQuantity;
        this.purchaseOrderAmount = purchaseOrderAmount;
        this.skuExpCnt = skuExpCnt;
        this.skuHitCnt = skuHitCnt;
        this.skuCartCnt = skuCartCnt;
        this.skuMarkedCnt = skuMarkedCnt;
        this.createOrderCnt = createOrderCnt;
        this.purchaseOrderCnt = purchaseOrderCnt;
    }



    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public String getDivice() {
        return divice;
    }

    public void setDivice(String divice) {
        this.divice = divice;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public Integer getEventYear() {
        return eventYear;
    }

    public void setEventYear(Integer eventYear) {
        this.eventYear = eventYear;
    }

    public Integer getEventMonth() {
        return eventMonth;
    }

    public void setEventMonth(Integer eventMonth) {
        this.eventMonth = eventMonth;
    }

    public String getEventDate() {
        return eventDate;
    }

    public void setEventDate(String eventDate) {
        this.eventDate = eventDate;
    }

    public Double getCreateOrderPrice() {
        return createOrderPrice;
    }

    public void setCreateOrderPrice(Double createOrderPrice) {
        this.createOrderPrice = createOrderPrice;
    }

    public Integer getCreateOrderQuantity() {
        return createOrderQuantity;
    }

    public void setCreateOrderQuantity(Integer createOrderQuantity) {
        this.createOrderQuantity = createOrderQuantity;
    }

    public Double getPurchaseOrderPrice() {
        return purchaseOrderPrice;
    }

    public void setPurchaseOrderPrice(Double purchaseOrderPrice) {
        this.purchaseOrderPrice = purchaseOrderPrice;
    }

    public Integer getPurchaseOrderQuantity() {
        return purchaseOrderQuantity;
    }

    public void setPurchaseOrderQuantity(Integer purchaseOrderQuantity) {
        this.purchaseOrderQuantity = purchaseOrderQuantity;
    }

    public Double getPurchaseOrderAmount() {
        return purchaseOrderAmount;
    }

    public void setPurchaseOrderAmount(Double purchaseOrderAmount) {
        this.purchaseOrderAmount = purchaseOrderAmount;
    }

    public Double getGmv() {
        return gmv;
    }

    public void setGmv(Double gmv) {
        this.gmv = gmv;
    }

    public Integer getSkuExpCnt() {
        return skuExpCnt;
    }

    public void setSkuExpCnt(Integer skuExpCnt) {
        this.skuExpCnt = skuExpCnt;
    }

    public Integer getSkuHitCnt() {
        return skuHitCnt;
    }

    public void setSkuHitCnt(Integer skuHitCnt) {
        this.skuHitCnt = skuHitCnt;
    }

    public Integer getSkuCartCnt() {
        return skuCartCnt;
    }

    public void setSkuCartCnt(Integer skuCartCnt) {
        this.skuCartCnt = skuCartCnt;
    }

    public Integer getSkuMarkedCnt() {
        return skuMarkedCnt;
    }

    public void setSkuMarkedCnt(Integer skuMarkedCnt) {
        this.skuMarkedCnt = skuMarkedCnt;
    }

    public Integer getCreateOrderCnt() {
        return createOrderCnt;
    }

    public void setCreateOrderCnt(Integer createOrderCnt) {
        this.createOrderCnt = createOrderCnt;
    }

    public Integer getPurchaseOrderCnt() {
        return purchaseOrderCnt;
    }

    public void setPurchaseOrderCnt(Integer purchaseOrderCnt) {
        this.purchaseOrderCnt = purchaseOrderCnt;
    }

    @Override
    public String toString() {
        return  site +
                "," + divice +
                "," + eventName  +
                "," + sku +
                "," + eventTime +
                "," + eventYear +
                "," + eventMonth +
                "," + eventDate +
                "," + createOrderPrice +
                "," + createOrderQuantity +
                "," + gmv +
                "," + purchaseOrderPrice +
                "," + purchaseOrderQuantity +
                "," + purchaseOrderAmount +
                "," + skuExpCnt +
                "," + skuHitCnt +
                "," + skuCartCnt +
                "," + skuMarkedCnt +
                "," + createOrderCnt +
                "," + purchaseOrderCnt ;
    }
}
