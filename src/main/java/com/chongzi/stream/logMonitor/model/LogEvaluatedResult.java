package com.chongzi.stream.logMonitor.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Data
@ToString
public class LogEvaluatedResult implements Serializable {

    private static final long serialVersionUID = -6636657895029200681L;
    /**
     * 应用名称
     */
    private String applicationName;
    /**
     * 监控事件详情集合
     */
    private List<LogEvent> logEvents;

}
