package io.github.luons.engine.core.spi;

import com.google.common.base.Preconditions;
import io.github.luons.engine.core.enums.Aggregations;
import lombok.Data;

import java.io.Serializable;

@Data
public class Column implements Serializable {
    /**
     * 列
     */
    private String column;
    /**
     * 别名
     */
    private String alias;
    /**
     * 聚类型
     */
    private Aggregations aggregation;
    /**
     * 是否是聚合列
     */
    private boolean isAggr;

    public Column(String column) {
        this(column, (null));
    }

    public Column(String column, String alias) {
        this(column, alias, (null), (false));
    }

    public Column(String column, String alias, String aggName) {
        Preconditions.checkNotNull(column, String.format("column %s must not be null", column));
        this.column = column;
        this.alias = alias;
        this.aggregation = Aggregations.getAggregationsType(aggName);
    }

    public Column(String column, String alias, Aggregations aggregation) {
        Preconditions.checkNotNull(column, String.format("column %s must not be null", column));
        this.column = column;
        this.alias = alias;
        this.aggregation = aggregation;
    }

    public Column(String column, String alias, boolean isAggr) {
        Preconditions.checkNotNull(column, String.format("column %s must not be null", column));
        this.column = column;
        this.alias = alias;
        this.isAggr = isAggr;
        if (this.alias == null) {
            this.alias = column;
        }
    }

    public Column(String column, String alias, Aggregations aggregation, boolean isAggr) {
        Preconditions.checkNotNull(column, String.format("column %s must not be null", column));
        this.column = column;
        this.alias = alias;
        this.isAggr = isAggr;
        if (this.alias == null) {
            this.alias = column;
        }
        this.aggregation = aggregation;
    }

}
