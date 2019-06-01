package com.chongzi.stream.dbus.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5Utils {
    public static String getMD5String(String str) {
        try {
            MessageDigest instance = MessageDigest.getInstance("MD5");
            byte[] digest = instance.digest(str.getBytes(StandardCharsets.UTF_8));

            StringBuffer sb = new StringBuffer();

            for (byte by : digest) {
                // 获取字节的低八位有效值
                int i = by & 0xff;
                // 将整数转为16进制
                String hexString = Integer.toHexString(i);

                if (hexString.length() < 2) {
                    // 如果是1位的话,补0
                    hexString = "0" + hexString;
                }
                sb.append(hexString);
            }

            return sb.toString();

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }

    }
}