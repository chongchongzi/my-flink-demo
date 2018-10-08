package com.chongzi.event;

/**
 * @Description 用户执行添加商品至购物车操作
 * @Author chongzi
 * @Date 2018/10/7 17:28
 **/
public class AfAddToBag {

    /**
     * 固定值，必须要为USD
     */
    private String af_currency;
    /**
     * 加购来源，枚举值有：unknowmediasource（加购来源较多，未能做到标识的暂时统一识别为
     * 未知来源）、recommend_homepage（主页推荐商品）、recommend productdetail（商详页底部的推荐商品）、
     * recommend_zme_exploreid_xxx(ZME explore中的帖子id(xxx表示帖子id)中对应的某商品)、
     * recommend_zme_outfitid_xxx(ZME outfit中的帖子id(xxx表示帖子id)中对应的某商品)、
     * category_xxx(某分类下商品,xxx表示分类名)、search_xxx(搜索指定条件商品，xxx表示关键字)、
     * recommend_zme_videoid_xxx(ZME video中的帖子id(xxx表示帖子id)中对应的某商品)、
     * promotion_xxx(营销活动名称,如主页配置的专题banner等)
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
     * 加购商品数量
     */
    private String af_quantity;
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
}
