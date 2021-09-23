package io.github.luons.engine.core.spi;

import com.google.common.base.Preconditions;
import lombok.Data;

import java.util.Map;

@Data
public class Dimension {

    /**
     * code
     */
    private String code;

    /**
     * column
     */
    private Column column;

    public Dimension(String code, String column) {
        Preconditions.checkNotNull(code, ("dimension code cannot be null"));
        Preconditions.checkNotNull(column, ("dimension column cannot be null"));
        this.code = code;
        this.column = new Column(column, column);
    }

    public Dimension(String code, Column column) {
        Preconditions.checkNotNull(code, ("dimension code cannot be null"));
        Preconditions.checkNotNull(column, ("dimension column cannot be null"));
        this.code = code;
        this.column = column;
    }

    public Object value(Map<String, Object> data) {
        return data.get(column.getAlias());
    }

}
