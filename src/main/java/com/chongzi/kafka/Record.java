package com.chongzi.kafka;

/**
 * @Description 日志记录实体类
 * @Author chongzi
 * @Date 2018/10/7 17:05
 **/
public class Record {
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
     * 事件内容
     */
    private String event_value;

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

    public String getEvent_value() {
        return event_value;
    }

    public void setEvent_value(String event_value) {
        this.event_value = event_value;
    }
}
