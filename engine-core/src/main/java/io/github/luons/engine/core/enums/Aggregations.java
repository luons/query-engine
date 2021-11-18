package io.github.luons.engine.core.enums;

import java.util.Arrays;

public enum Aggregations {
    /**
     * 聚合类型
     */
    Average("average"),
    Avg("avg"),
    COUNT("count"),
    UNIQUE_COUNT("unique_count"),
    MAX("max"),
    MEDIAN("median"),
    MIN("min"),
    SUM("sum"),
    /**
     * rate 只应用于时间聚合
     */
    RATE("rate"),
    /**
     * derivative 只应用于时间聚合
     */
    DERIVATIVE("derivative"),
    DEFAULT("average"),
    ;
    private String agg;

    private Aggregations(String agg) {
        this.agg = agg;
    }

    public static Aggregations getAggregationsType(String aggName) {
        return Arrays.stream(Aggregations.values())
                .filter(agg -> agg.getAgg().equalsIgnoreCase(aggName))
                .findFirst().orElse(Aggregations.DEFAULT);
    }

    public String getAgg() {
        return agg;
    }

    public void setAgg(String agg) {
        this.agg = agg;
    }
}
