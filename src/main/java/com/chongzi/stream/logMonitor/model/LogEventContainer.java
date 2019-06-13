package com.chongzi.stream.logMonitor.model;

import lombok.Data;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Data
@ToString
public class LogEventContainer {
    public String key;
    public long startTime;
    public long lastModified;
    public long count;
    private List<LogEvent> logEvents = new ArrayList<>();
}
