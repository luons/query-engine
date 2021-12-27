package io.github.luons.engine.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.type.MapType;
import com.google.common.base.CaseFormat;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JacksonUtils {

    /**
     * common mapper
     */
    public static final ObjectMapper MAPPER = newMapper();

    public static final MapType TYPE_MAP = MAPPER.getTypeFactory().constructMapType(HashMap.class, Object.class, Object.class);

    /**
     * 创建新的 ObjectMapper
     */
    public static ObjectMapper newMapper() {
        return new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    /**
     * 对象转为json字符串
     */
    public static <T> String toJson(T obj) {
        if (obj == null) {
            return "{}";
        } else if (obj instanceof String) {
            return (String) obj;
        }
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("JsonUtil.toJsonString is exception " + e);
        }
        return obj.toString();
    }

    /**
     *
     */
    public static <T> String toJson(T obj, boolean mapper) {
        try {
            if (mapper) {
                ObjectMapper mapper1 = new ObjectMapper()
                        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
                return mapper1.writeValueAsString(obj);
            }
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("JsonUtil.toJsonString is exception " + e);
        }
        return "";
    }

    /**
     * 将数据转换为对象列表
     */
    public static <T> List<T> parseAsList(Object obj, Class<T> itemType) {
        if (obj == null) {
            return null;
        } else if (obj instanceof String) {
            return parseAsList(obj.toString(), itemType);
        }
        return parseAsList(toJson(obj), itemType);
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

    public static Map parseAsMap(String json) {
        return parse(json, TYPE_MAP);
    }

    public static Map<String, Object> parseAsMapKeyString(String json) {
        return parse(json, TYPE_MAP);
    }

    public static Map<String, Object> bean2Map(Object obj) {
        if (obj == null) {
            return null;
        }
        return parse(toJson(obj), TYPE_MAP);
    }

    /**
     * json 格式化为下划线
     */
    public static Map<String, Object> parseAsMapCamel2Under(String json) {
        Map<String, Object> data = parseAsMapKeyString(json);
        if (data != null && data.size() > 0) {
            return parseMapKey2Under(data);
        }
        return null;
    }

    /**
     * map key 键驼峰转下划线
     */
    public static Map<String, Object> parseMapKey2Under(Map<String, Object> map) {
        if (map == null || map.size() == 0) {
            return map;
        }
        Map<String, Object> data = new HashMap<>(map.size());
        map.forEach((key, value) -> data.put(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key), value));
        return data;
    }
}
