package com.chongzi.stream.logMonitor.model;

import lombok.Data;
import lombok.ToString;

import java.io.Serializable;

/**
 * @Description 日志配置
 * @Author chongzi
 * @Date 2019/5/28 19:51
 * @Param 
 * @return 
 **/
@Data
@ToString
public class LogConfig implements Serializable {
    private static final long serialVersionUID = 4416826133199103653L;

    /**
     * 应用名称
     */
    private String applicationName;

    /**
     * 应用描述
     */
    private String applicationRemarks;

    /**
     * 告警关键字正则
     */
    private String alarmKeywordRule;

    /**
     * 告警描述
     */
    private String alarmRemarks;

    /**
     * 告警类型(1=实时告警，2=聚合告警)
     */
    private Integer alarmType;

    /**
     * 规则创建时间
     */
    private String registerDate;
    /**
     * 时长(秒)
     */
    private Integer timeLength;

    /**
     * 次数
     */
    private Integer num;

    /**
     * 告警邮件
     */
    private String email;

    /**
     * 告警VV
     */
    private String vv;

    /**
     * 告警微信
     */
    private String weixin;

    public LogConfig() {
    }

    public LogConfig(String applicationName, String applicationRemarks, String alarmKeywordRule, String alarmRemarks, Integer alarmType, String registerDate, Integer timeLength, Integer num, String email, String vv, String weixin) {
        this.applicationName = applicationName;
        this.applicationRemarks = applicationRemarks;
        this.alarmKeywordRule = alarmKeywordRule;
        this.alarmRemarks = alarmRemarks;
        this.alarmType = alarmType;
        this.registerDate = registerDate;
        this.timeLength = timeLength;
        this.num = num;
        this.email = email;
        this.vv = vv;
        this.weixin = weixin;
    }
}
