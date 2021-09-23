package io.github.luons.engine.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

@Data
@Slf4j
public class ServiceException extends RuntimeException {

    private static final String DEFAULT_ERROR_MESSAGE = "请稍候再重试";

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static Map<String, Map<String, String>> messageMap = new HashMap<>();

    private static Map<String, String> zhMessageMap = new HashMap<>();
    private static Map<String, String> enMessageMap = new HashMap<>();

    static {
        messageMap.put("en", enMessageMap);
        messageMap.put("zh", zhMessageMap);
        try {
            {
                Properties zhMessageProp = new Properties();
                List<InputStream> zhResources = loadResources("message_zh_CN.properties", (null));
                for (InputStream inputStream : zhResources) {
                    zhMessageProp.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                }
                for (Entry<Object, Object> entry : zhMessageProp.entrySet()) {
                    zhMessageMap.put((String) entry.getKey(), (String) entry.getValue());
                }
            }
            {
                Properties enMessageProp = new Properties();
                List<InputStream> enResources = loadResources("message_en_US.properties", (null));
                for (InputStream inputStream : enResources) {
                    enMessageProp.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
                }
                for (Entry<Object, Object> entry : enMessageProp.entrySet()) {
                    enMessageMap.put((String) entry.getKey(), (String) entry.getValue());
                }
            }
        } catch (Exception e) {
            log.error("Fail to load message properties: ", e);
        }
    }

    public static ServiceException createDefault() {
        return create(MessageWrapper.SC_INTERNAL_SERVER_ERROR, ("unknown.error.message"));
    }

    public static ServiceException create(int code, String property) {
        return create(code, property, null);
    }

    public static ServiceException create(int code, String property, Exception cause, Object... args) {
        String errorMessage = getErrorMessage(property);
        if (errorMessage != null) {
            if (args != null && args.length > 0) {
                errorMessage = String.format(errorMessage, args);
            }
            return new ServiceException(code, errorMessage, cause);
        } else {
            log.warn("Fail to message for property: {} , in language: {}", property, UserLanguage.getLanguage());
            return new ServiceException(code, DEFAULT_ERROR_MESSAGE, cause);
        }
    }

    private ServiceException(int code, String message, Exception cause) {
        super(cause);
        this.code = code;
        this.message = message;
    }

    private final int code;

    private final String message;

    private String data;

    public String toJsonString() {
        try {
            Map<String, Object> map = new HashMap<>();
            map.put("code", this.getCode());
            map.put("message", this.getMessage());
            map.put("data", this.getData());
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            log.error("Fail to write object into json string: " + e);
            return null;
        }
    }

    public static String getErrorMessage(String property) {
        String language = UserLanguage.getLanguage();
        Map<String, String> languageMap = getLanguageMap(language);
        return languageMap.get(property);
    }

    public static List<InputStream> loadResources(final String name, final ClassLoader classLoader) throws IOException {
        final List<InputStream> list = new ArrayList<>();
        final Enumeration<URL> systemResources =
                (classLoader == null ? ClassLoader.getSystemClassLoader() : classLoader).getResources(name);
        while (systemResources.hasMoreElements()) {
            list.add(systemResources.nextElement().openStream());
        }
        return list;
    }

    private static Map<String, String> getLanguageMap(String language) {
        if (messageMap.containsKey(language)) {
            return messageMap.get(language);
        } else {
            log.warn("Fail to get language map, for language: {}", language);
            return zhMessageMap;
        }
    }

}

