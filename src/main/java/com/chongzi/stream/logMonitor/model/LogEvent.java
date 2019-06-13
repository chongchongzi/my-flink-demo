package com.chongzi.stream.logMonitor.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @Description 日志事件
 * @Author chongzi
 * @Date 2019/5/28 19:50
 * @Param 
 * @return 
 **/
@Data
@ToString
public class LogEvent implements Serializable {

    private static final long serialVersionUID = -52934893611845880L;
    /**
     * 应用名称
     */
    public String applicationName;
    /**
     * 事件时间
     */
    public long eventTime;
    /**
     * log数据
     */
    public String log;

    public LogEvent(String applicationName,  long eventTime, String log) {
        this.applicationName = applicationName;
        this.eventTime = eventTime;
        this.log = log;
    }

    public static LogEvent of(String applicationName, long eventTime, String log){
        return new LogEvent(applicationName,eventTime,log);
    }

}
