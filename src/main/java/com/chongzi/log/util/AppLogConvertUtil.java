package com.chongzi.log.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AppLogConvertUtil {

    public static final String PARAMETERS_PATTERN = "_app.gif\\??(.*)HTTP";
    //public static final String TIMESTAMP_PATTERN = "\\^A\\^\\d{10}";

    public static final String TIMESTAMP_KEY = "timestamp";

    /**
     * 将原始埋点数据转换为 map
     * app 数据
     * @param log
     * @return
     */
    public static Map<String, Object> getAppLogParameters(String log) {
        if (log.contains("/_app.gif")) {
            Pattern p = Pattern.compile(PARAMETERS_PATTERN);
            Matcher m = p.matcher(log);
            String requestStr = "";

            while (m.find()) {
                requestStr = m.group();
            }
            // String decodedUrl = URLDecoder.decode(requestStr, "utf-8");
            Map<String, Object> map = NginxLogConvertUtil.getUrlParams(requestStr);
            if (map.size() > 0) {
                map.put(TIMESTAMP_KEY, NginxLogConvertUtil.getTimestamp(log));
                return map;
            }
        }
        return null;
    }

}
