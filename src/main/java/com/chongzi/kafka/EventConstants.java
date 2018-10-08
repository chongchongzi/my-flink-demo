package com.chongzi.kafka;

/**
 * @Description 事件常量
 * @Author chongzi
 * @Date 2018/10/7 17:55
 **/
public class EventConstants {
    /**
     * 用户注册或登录成功后调用（包含FACEBOOK,GOOGLE登录）
     */
    public final static String AF_SIGN_UP = "af_sign_up";
    /**
     * 用户执行添加商品至购物车操作
     */
    public final static String AF_ADD_TO_BAG = "af_add_to_bag";
    /**
     * 用户从购物车进入checkout页，checkout页需要获取接口数据成功后触发
     * 此事件（修改地址、使用优惠券也会调用）
     */
    public final static String AF_PROCESS_TO_PAY = "af_process_to_pay";
    /**
     * 创建订单成功
     */
    public final static String AF_CREATE_ORDER_SUCCESS = "af_create_order_success";
    /**
     * 订单成功支付
     */
    public final static String AF_PURCHASE = "af_purchase";
    /**
     * 进入产品详情页查看指定sku商品
     */
    public final static String AF_VIEW_PRODUCT = "af_view_product";
    /**
     * 用户执行搜索操作（包含有普通搜索或图片搜索）
     */
    public final static String AF_SEARCH = "af_search";
    /**
     * 进入查看某个推广活动（无意义事件，可忽略）
     */
    public final static String AF_VIEW_LIST = "af_view_list";
    /**
     * 添加商品至收藏夹
     */
    public final static String AF_ADD_TO_WISHLIST = "af_add_to_wishlist";
    /**
     * 发布帖子（普通帖子或穿搭）
     */
    public final static String AF_POST = "af_post";
    /**
     * 查看帖子
     */
    public final static String AF_POST_VIEW = "af_post_view";
    /**
     * 用户通过 facebook或messger进行分享
     */
    public final static String AF_SHARE = "af_share";
    /**
     * 列表商品展示（来源会有多处，如首页推荐位，商详推荐位，explore栏
     * 的帖子详情中的商品列表等）
     */
    public final static String AF_IMPRESSION = "af_impression";
}
