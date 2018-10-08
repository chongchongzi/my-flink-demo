package com.chongzi.event;

/**
 * @Description 添加商品至收藏夹
 * @Author chongzi
 * @Date 2018/10/7 17:30
 **/
public class AfAddToWishlist {

    /**
     * 固定值，必须要为USD
     */
    private String af_currency;
    /**
     * carts page（购物车中商品列表）,full sale gift（满赠商品列表）,recommend_homepage(主页推荐商品列表),pic search(图片搜索商品列表),product detail（商详页中具体商品）
     */
    private String af_inner_mediasource;
    /**
     * 商品所属分类
     */
    private String af_content_category;
    /**
     * 商品价格，必须要为USD 美金汇率价格
     */
    private String af_price;
    /**
     * 商品sku(切勿使用goods_id)
     */
    private String af_content_id;
    /**
     * 固定值，必须为prodcut
     */
    private String af_content_type;

    public String getAf_currency() {
        return af_currency;
    }

    public void setAf_currency(String af_currency) {
        this.af_currency = af_currency;
    }

    public String getAf_inner_mediasource() {
        return af_inner_mediasource;
    }

    public void setAf_inner_mediasource(String af_inner_mediasource) {
        this.af_inner_mediasource = af_inner_mediasource;
    }

    public String getAf_content_category() {
        return af_content_category;
    }

    public void setAf_content_category(String af_content_category) {
        this.af_content_category = af_content_category;
    }

    public String getAf_price() {
        return af_price;
    }

    public void setAf_price(String af_price) {
        this.af_price = af_price;
    }

    public String getAf_content_id() {
        return af_content_id;
    }

    public void setAf_content_id(String af_content_id) {
        this.af_content_id = af_content_id;
    }

    public String getAf_content_type() {
        return af_content_type;
    }

    public void setAf_content_type(String af_content_type) {
        this.af_content_type = af_content_type;
    }
}
