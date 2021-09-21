package com.luons.engine.core.spi;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 指标最小维度集
 */
public interface IMeasureMinDimSets {

    /**
     * get方法
     *
     * @return Map<String, List < Set < String>>> key：measureCode大写，value：最小维度集的list
     */
    Map<String, List<Set<String>>> getMeasureMinDimSetsMap();

    /**
     * Set 方法
     *
     * @param measureSetsMap key：measureCode大写，value：最小维度集的list
     */
    void setMeasureMinDimSetsMap(Map<String, List<Set<String>>> measureSetsMap);
}
