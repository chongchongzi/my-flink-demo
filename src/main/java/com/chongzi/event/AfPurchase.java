package com.chongzi.event;

/**
 * @Description 订单成功支付
 * @Author chongzi
 * @Date 2018/10/7 17:29
 **/
public class AfPurchase {

    /**
     * 固定值，必须要为USD
     */
    private String af_currency;
    /**
     * 带来的收益，必须要为USD 美金汇率价格
     */
    private String af_revenue;
    /**
     * 此订单中的商品的价格，多个用,分隔
     */
    private String af_price;
    /**
     * 订单号(非订单id)
     */
    private String af_reciept_id;
    /**
     * 此订单中的商品的sku(切勿使用goods_id)，多个用,分隔
     */
    private String af_content_id;
    /**
     * 此订单中的商品的购买数量，多个用,分隔
     */
    private String af_quantity;
    /**
     * 固定值，必须为prodcut
     */
    private String af_content_type;
    /**
     * 用户选择的支付方式，枚举值有PayPal、WorldPay、Cash On Delivery、boletoBancario、CheckoutCredit
     */
    private String af_payment;

    public String getAf_currency() {
        return af_currency;
    }

    public void setAf_currency(String af_currency) {
        this.af_currency = af_currency;
    }

    public String getAf_revenue() {
        return af_revenue;
    }

    public void setAf_revenue(String af_revenue) {
        this.af_revenue = af_revenue;
    }

    public String getAf_price() {
        return af_price;
    }

    public void setAf_price(String af_price) {
        this.af_price = af_price;
    }

    public String getAf_reciept_id() {
        return af_reciept_id;
    }

    public void setAf_reciept_id(String af_reciept_id) {
        this.af_reciept_id = af_reciept_id;
    }

    public String getAf_content_id() {
        return af_content_id;
    }

    public void setAf_content_id(String af_content_id) {
        this.af_content_id = af_content_id;
    }

    public String getAf_quantity() {
        return af_quantity;
    }

    public void setAf_quantity(String af_quantity) {
        this.af_quantity = af_quantity;
    }

    public String getAf_content_type() {
        return af_content_type;
    }

    public void setAf_content_type(String af_content_type) {
        this.af_content_type = af_content_type;
    }

    public String getAf_payment() {
        return af_payment;
    }

    public void setAf_payment(String af_payment) {
        this.af_payment = af_payment;
    }
}
