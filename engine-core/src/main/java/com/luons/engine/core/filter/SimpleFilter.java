package com.luons.engine.core.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.luons.engine.core.enums.Operator;
import lombok.Data;

@Data
@JsonDeserialize()
public class SimpleFilter implements Filter {

    private String name;
    private Operator operator;
    private Object value;

    @JsonCreator
    public SimpleFilter(@JsonProperty("name") String name,
                        @JsonProperty("operator") Operator operator,
                        @JsonProperty("value") Object value) {
        this.name = name;
        this.operator = operator;
        this.value = value;
    }

    public SimpleFilter(String name, Object value) {
        this.value = value;
        this.operator = Operator.EQ;
        this.name = name;
    }

}
