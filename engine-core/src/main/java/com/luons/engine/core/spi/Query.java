package com.luons.engine.core.spi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.luons.engine.common.Pageable;
import com.luons.engine.core.filter.FilterGroup;
import lombok.Data;

import java.io.Serializable;
import java.util.*;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Query implements Serializable {

    /**
     * filter WHERE condition
     */
    private FilterGroup filterGroup;
    /**
     * CASE WHEN condition
     */
    private List<RangeDim> rangeDims = new LinkedList<>();
    /**
     * need fields
     */
    private LinkedHashSet<String> fields = new LinkedHashSet<>();
    /**
     * dimension, group by fields
     */
    private LinkedHashSet<String> dimensions = new LinkedHashSet<>();
    /**
     * measures(sum/count/count distinct)
     */
    private LinkedHashSet<String> measures = new LinkedHashSet<>();
    /**
     * order fields
     */
    private LinkedHashSet<String> orders = new LinkedHashSet<>();
    /**
     * 备注
     */
    private LinkedHashSet<String> hints = new LinkedHashSet<>();
    /**
     * 分页
     */
    private Pageable pageable;


    public Query dimension(String dimension) {
        this.dimensions.add(dimension);
        return this;
    }

    public Query measure(String measure) {
        this.measures.add(measure);
        return this;
    }

    public Query filterGroup(FilterGroup filterGroup) {
        this.filterGroup = filterGroup;
        return this;
    }

    public Query pageable(Pageable pageable) {
        this.pageable = pageable;
        return this;
    }

    public Query field(String field) {
        this.fields.add(field);
        return this;
    }

    public Query order(String order) {
        this.orders.add(order);
        return this;
    }

    public Query hint(String hint) {
        this.hints.add(hint);
        return this;
    }

    public Query rangeDim(RangeDim rangeDim) {
        this.rangeDims.add(rangeDim);
        return this;
    }

    public Query cloneBasicQuery() {
        Query query = new Query();
        query.dimensions = this.dimensions;
        query.measures = this.measures;
        query.filterGroup = this.filterGroup;
        query.fields = this.fields;
        query.orders = this.orders;
        query.hints = this.hints;
        query.rangeDims = this.rangeDims;
        query.pageable = this.pageable;
        return query;
    }

    public boolean isEmpty(Collection<?> collection) {
        if (Objects.isNull(collection)) {
            return true;
        }
        return collection.isEmpty();
    }

    @Data
    public static class RangeDim {

        private String dimCode;

        private List<Object> splits;
    }
}
