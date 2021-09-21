package com.luons.engine.core.spi;

import com.google.common.base.Preconditions;
import lombok.Data;

import java.util.Map;

@Data
public class Measure {

    /**
     * code
     */
    private String code;

    /**
     * Column[]
     */
    private Column[] columns;

    private Calculator calculator = new Calculator() {
        @Override
        public double value(Map<String, Object> data) {
            return doubleValue(data, columns[0].getAlias());
        }
    };

    public Measure(String code, String[] columns) {
        this(code, columns, null);
    }

    public Measure(String code, Column[] columns) {
        this(code, columns, null);
    }

    public Measure(String code, String[] columns, Calculator calculator) {

        Preconditions.checkNotNull(code, ("measure code can't be null"));
        Preconditions.checkArgument((columns != null && columns.length > 0), ("measure columns should not be empty"));
        this.code = code;
        this.columns = new Column[columns.length];
        for (int i = 0; i < columns.length; i++) {
            this.columns[i] = new Column(columns[i], columns[i]);
        }
        if (calculator != null) {
            this.calculator = calculator;
        }
    }

    public Measure(String code, Column[] columns, Calculator calculator) {
        Preconditions.checkNotNull(code, ("measure code can't be null"));
        Preconditions.checkArgument((columns != null && columns.length > 0), ("measure columns should not be empty"));
        this.code = code;
        this.columns = columns;
        if (calculator != null) {
            this.calculator = calculator;
        }
    }

    public static double doubleValue(Map<String, Object> data, String column) {
        Object obj = data.get(column);
        if (obj == null) {
            return 0.0;
        }
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        }
        throw new RuntimeException(String.format("invalid data type for column: %s(%s)", column, obj));
    }

    public double value(Map<String, Object> data) {
        return calculator.value(data);
    }
}
