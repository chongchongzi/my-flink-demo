package com.chongzi.log.util;

import org.apache.commons.lang3.StringUtils;

public class SiteUtil {

    public static String getAppSite(String appName) {
        if (StringUtils.isNotEmpty(appName)) {
            String lowerName = appName.toLowerCase();
            if (lowerName.contains("zaful")) {
                return "zaful";
            }
            if (lowerName.contains("gearbest")) {
                return "GB";
            }
            if (lowerName.contains("rosegal")) {
                return "RG";
            }
        }
        return "";
    }

}
