package com.chongzi.log.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NginxLogConvertUtil {

    private static final Logger logger = LoggerFactory.getLogger(NginxLogConvertUtil.class);


    public static final String PARAMETERS_PATTERN = "_ubc.gif\\??(.*)HTTP";
    public static final String TIMESTAMP_PATTERN = "\\^A\\^\\d{10}";

    public static final String TIMESTAMP_KEY = "timestamp";

    /**
     * 将原始埋点数据转换为 map
     *
     * @param log
     * @return
     */
    public static Map<String, Object> getNginxLogParameters(String log) {
        Pattern p = Pattern.compile(PARAMETERS_PATTERN);
        Matcher m = p.matcher(log);
        String requestStr = "";

        while (m.find()) {
            requestStr = m.group();
        }
        // String decodedUrl = URLDecoder.decode(requestStr, "utf-8");
        Map<String, Object> map = getUrlParams(requestStr);
        if (map.size() > 0) {
            map.put(TIMESTAMP_KEY, getTimestamp(log));
            return map;
        }

        return null;
    }

    public static Long getTimestamp(String log) {
        Pattern p = Pattern.compile(TIMESTAMP_PATTERN);
        Matcher m = p.matcher(log);
        String requestStr = "";

        while (m.find()) {
            requestStr = m.group();
        }
        if (StringUtils.isNotEmpty(requestStr)) {
            String pattern = "\\d";
            Pattern p1 = Pattern.compile(pattern);
            Matcher m1 = p1.matcher(requestStr);
            StringBuilder time = new StringBuilder();
            while (m1.find()) {
                time.append(m1.group());
            }
            return Long.valueOf(time.toString()) * 1000;
        }

        return System.currentTimeMillis();
    }

    public static Map<String, Object> getUrlParams(String param) {
        Map<String, Object> map = new HashMap(0);
        if (StringUtils.isBlank(param)) {
            return map;
        }
        String[] params = param.split("&");
        for (int i = 0; i < params.length; i++) {
            String[] p = params[i].split("=");
            if (p.length == 2) {
                try {
                    String key = p[0];
                    if (key.startsWith("_ubc.gif?")) {
                        map.put(key.replace("_ubc.gif?", "").toLowerCase(), URLDecoder.decode(p[1], "utf-8"));
                    } else {

                        map.put(p[0].toLowerCase(), valueHex(URLDecoder.decode(p[1], "utf-8")));

                    }
                } catch (Exception e) {
                    logger.error("获取 url 参数出错,value:{}", params, e);
                }
            }
        }
        return map;
    }

    public static Object valueHex(Object o) {
        String s = String.valueOf(o);
        if (StringUtils.isNotEmpty(s) && (s.contains("\\x22") || s.contains("\\\\x22"))) {
            return s.replaceAll("\\\\x22", "\"").replaceAll("\\x22", "\"");
        }
        return o;
    }
}
