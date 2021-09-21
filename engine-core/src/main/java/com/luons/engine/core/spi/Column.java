package com.luons.engine.core.spi;

import com.google.common.base.Preconditions;
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
     * 是否是聚合列
     */
    private boolean isAggr;

    public Column(String column, String alias, boolean isAggr) {
        Preconditions.checkNotNull(column, String.format("column %s must not be null", column));
        this.column = column;
        this.alias = alias;
        this.isAggr = isAggr;
        if (this.alias == null) {
            this.alias = column;
        }
    }

    public Column(String column) {
        this(column, (null));
    }

    public Column(String column, String alias) {
        this(column, alias, (false));
    }

}
