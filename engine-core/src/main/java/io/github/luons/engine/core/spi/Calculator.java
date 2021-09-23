package io.github.luons.engine.core.spi;

import java.util.Map;

public interface Calculator {

    /**
     * 值转换
     *
     * @param data data
     * @return double
     */
    double value(Map<String, Object> data);
}
