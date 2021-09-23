package io.github.luons.engine.core.cube;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;

public class CubeMap<T> extends LinkedHashMap<String, T> {

    @Override
    public T put(String key, T value) {
        if (StringUtils.isNotBlank(key)) {
            return super.put(key.toUpperCase(), value);
        }
        return super.put("NULL", value);
    }

    @Override
    public T get(Object key) {
        if (key instanceof String) {
            String ks = (String) key;
            return super.get(ks.toUpperCase());
        }
        return super.get(key);
    }
}
