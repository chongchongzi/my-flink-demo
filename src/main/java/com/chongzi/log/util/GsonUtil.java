package com.chongzi.log.util;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonUtil {

    private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();


    public static <T> T readValue(String jsonStr, Class<T> valueType) {
        return gson.fromJson(jsonStr, valueType);
    }

    public static String toJson(Object o) {
        return gson.toJson(o);
    }

}
