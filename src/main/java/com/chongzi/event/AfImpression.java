package com.chongzi.event;

/**
 * @Description 列表商品展示（来源会有多处，如首页推荐位，商详推荐位，explore栏 的帖子详情中的商品列表等）
 * @Author chongzi
 * @Date 2018/10/7 17:31
 **/
public class AfImpression {

    /**
     * 枚举值有：recommend_homepage（首页推荐位）,recommend productdetail（商详页推荐位）,
     * recommend_zme_exploreid_xxx（入口为explore），recommend_zme_outfitid_xxx（入口为outfit）
     * recommend_zme_videoid_xxx（入口为outfit），category_xxx（分类），promotion_（营销活动）
     * search_xxx(搜索)
     */
    private String af_inner_mediasource;
    /**
     * 此订单中的商品的sku(切勿使用goods_id)，多个用,分隔
     */
    private String af_content_id;


    public String getAf_inner_mediasource() {
        return af_inner_mediasource;
    }

    public void setAf_inner_mediasource(String af_inner_mediasource) {
        this.af_inner_mediasource = af_inner_mediasource;
    }

    public String getAf_content_id() {
        return af_content_id;
    }

    public void setAf_content_id(String af_content_id) {
        this.af_content_id = af_content_id;
    }
}
