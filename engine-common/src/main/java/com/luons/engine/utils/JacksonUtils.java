package com.luons.engine.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JacksonUtils {

    private static final ObjectMapper MAPPER = newMapper();

    private static final MapType TYPE_MAP = MAPPER.getTypeFactory().constructMapType(HashMap.class, Object.class, Object.class);

    public ObjectMapper getObjectMapper() {
        return MAPPER;
    }

    /**
     * 创建新的 ObjectMapper
     */
    private static ObjectMapper newMapper() {
        return new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * 对象转为json字符串
     */
    public static <T> String toJsonString(T obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("JsonUtil.toJsonString is exception " + e);
        }
        return "";
    }

    /**
     * 将 json 字符串转为对象列表
     */
    public static <T> List<T> parseAsList(String json, Class<T> itemType) {
        return parse(json, MAPPER.getTypeFactory().constructCollectionType(List.class, itemType));
    }

    /**
     * json字符串转为指定类型
     */
    public static <T> T parse(String json, Class<T> type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            log.error("JsonUtil.parse is exception " + e);
        }
        return null;
    }

    /**
     * 将JSON字符串转为Java对象
     */
    public static <T> T parse(String json, JavaType type) {
        try {
            return MAPPER.readValue(json, type);
        } catch (IOException e) {
            log.error("JsonUtil.parse{} is exception " + e, json);
        }
        return null;
    }

    public static Map<String, Object> parseAsMap(String json) {
        return parse(json, TYPE_MAP);
    }

}
