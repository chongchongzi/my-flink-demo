package com.chongzi.bean;

import java.util.Date;

public class PublicDailyStatisticsOrder {

    private int id;
    private String the_date_cd;
    private String sku;
    private String spu;
    private String website_code;
    private String site_code;
    private String terminal;
    private String country;
    private int real_qty;
    private int order_qty;
    private Float sale_account_usd;
    private String etl_date;
    private Date dw_sys_date;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getThe_date_cd() {
        return the_date_cd;
    }

    public void setThe_date_cd(String the_date_cd) {
        this.the_date_cd = the_date_cd;
    }

    public String getSku() {
        return sku;
    }

    public void setSku(String sku) {
        this.sku = sku;
    }

    public String getSpu() {
        return spu;
    }

    public void setSpu(String spu) {
        this.spu = spu;
    }

    public String getWebsite_code() {
        return website_code;
    }

    public void setWebsite_code(String website_code) {
        this.website_code = website_code;
    }

    public String getSite_code() {
        return site_code;
    }

    public void setSite_code(String site_code) {
        this.site_code = site_code;
    }

    public String getTerminal() {
        return terminal;
    }

    public void setTerminal(String terminal) {
        this.terminal = terminal;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public int getReal_qty() {
        return real_qty;
    }

    public void setReal_qty(int real_qty) {
        this.real_qty = real_qty;
    }

    public int getOrder_qty() {
        return order_qty;
    }

    public void setOrder_qty(int order_qty) {
        this.order_qty = order_qty;
    }

    public Float getSale_account_usd() {
        return sale_account_usd;
    }

    public void setSale_account_usd(Float sale_account_usd) {
        this.sale_account_usd = sale_account_usd;
    }

    public String getEtl_date() {
        return etl_date;
    }

    public void setEtl_date(String etl_date) {
        this.etl_date = etl_date;
    }

    public Date getDw_sys_date() {
        return dw_sys_date;
    }

    public void setDw_sys_date(Date dw_sys_date) {
        this.dw_sys_date = dw_sys_date;
    }

    @Override
    public String toString() {
        return "PublicDailyStatisticsOrder{" +
                "id=" + id +
                ", the_date_cd='" + the_date_cd + '\'' +
                ", sku='" + sku + '\'' +
                ", spu='" + spu + '\'' +
                ", website_code='" + website_code + '\'' +
                ", site_code='" + site_code + '\'' +
                ", terminal='" + terminal + '\'' +
                ", country='" + country + '\'' +
                ", real_qty=" + real_qty +
                ", order_qty=" + order_qty +
                ", sale_account_usd=" + sale_account_usd +
                ", etl_date='" + etl_date + '\'' +
                ", dw_sys_date=" + dw_sys_date +
                '}';
    }
}
