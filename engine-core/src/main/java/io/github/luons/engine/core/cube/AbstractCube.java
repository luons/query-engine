package io.github.luons.engine.core.cube;

import io.github.luons.engine.core.filter.Filter;
import io.github.luons.engine.core.filter.FilterGroup;
import io.github.luons.engine.core.filter.SimpleFilter;
import io.github.luons.engine.core.spi.Dimension;
import io.github.luons.engine.core.spi.Measure;
import io.github.luons.engine.core.spi.Query;
import io.github.luons.engine.core.utils.CoreUtils;
import lombok.Data;

import java.util.*;

@Data
public abstract class AbstractCube implements ICube {

    protected String cubeName;

    protected String granularity;

    protected FilterGroup limitFilterGroup;

    protected CubeMap<Dimension> dimensions = new CubeMap<>();

    protected CubeMap<Measure> measures = new CubeMap<>();

    public AbstractCube name(String cubeName) {
        this.cubeName = cubeName;
        return this;
    }

    public AbstractCube limit(FilterGroup limitGroup) {
        this.limitFilterGroup = limitGroup;
        return this;
    }

    public AbstractCube dimension(Dimension dimension) {
        this.dimensions.put(dimension.getCode().toUpperCase(), dimension);
        return this;
    }

    public AbstractCube measure(Measure measure) {
        this.measures.put(measure.getCode().toUpperCase(), measure);
        return this;
    }

    public AbstractCube granularity(String granularity) {
        this.granularity = granularity;
        return this;
    }

    public void validateQuery(Query query) {
        if (Objects.isNull(query)) {
            throw new IllegalArgumentException("query is empty !");
        }
        if (!Objects.isNull(query.getFilterGroup())) {
            validateFilterGroup(query.getFilterGroup());
        }
        if (!Objects.isNull(query.getDimensions())) {
            for (String dimension : query.getDimensions()) {
                if (dimensions.get(dimension) == null) {
                    throw new IllegalArgumentException(String.format("invalid dimension [%s] for cube [%s]",
                            dimension, cubeName));
                }
            }
        }
        if (!Objects.isNull(query.getMeasures())) {
            for (String measure : query.getMeasures()) {
                if (measures.get(measure.toUpperCase()) == null) {
                    throw new IllegalArgumentException(String.format("invalid measure [%s] for cube [%s]",
                            measure, cubeName));
                }
            }
        }
        if (!Objects.isNull(query.getOrders())) {
            for (String ord : query.getOrders()) {
                String s = ord.replaceFirst(("[+\\-]"), ("")).trim().toUpperCase();
                if (dimensions.get(s.toUpperCase()) == null && measures.get(s.toUpperCase()) == null) {
                    throw new IllegalArgumentException(String.format("invalid order [%s] for cube [%s]", s, cubeName));
                }
            }
        }
    }

    public void validateFilterGroup(FilterGroup filterGroup) {
        if (Objects.isNull(filterGroup) || filterGroup.getFilterList() == null) {
            return;
        }
        for (Filter f : filterGroup.getFilterList()) {
            if (f instanceof FilterGroup) {
                validateFilterGroup((FilterGroup) f);
            } else if (f instanceof SimpleFilter) {
                String s = ((SimpleFilter) f).getName().toUpperCase();
                if (dimensions.get(s) == null && measures.get(s) == null) {
                    throw new IllegalArgumentException(String.format("invalid filter %s for cube %s", s, cubeName));
                }
            } else {
                throw new IllegalArgumentException(String.format("unsupported filter type %s", f.getClass().getName()));
            }
        }
    }

    public List<Map<String, Object>> toMetric(List<CubeMap<Object>> rawMetrics, Query query) {
        List<Map<String, Object>> metricsMapList = new LinkedList<>();
        if (rawMetrics == null) {
            return metricsMapList;
        }
        for (CubeMap<Object> rawMetric : rawMetrics) {
            if (rawMetric == null) {
                continue;
            }
            Map<String, Object> itm = new LinkedHashMap<>();
            for (String dimension : query.getDimensions()) {
                itm.put(dimension, dimensions.get(dimension).value(rawMetric));
            }
            for (Query.RangeDim rangeDim : query.getRangeDims()) {
                String dimCode = rangeDim.getDimCode();
                itm.put(dimCode, dimensions.get(dimCode).value(rawMetric));
            }
            for (String measure : query.getMeasures()) {
                itm.put(measure, measures.get(measure).value(rawMetric));
            }
            for (String field : query.getFields()) {
                itm.put(field, dimensions.get(field).value(rawMetric));
            }
            metricsMapList.add(itm);
        }
        return metricsMapList;
    }

    /**
     * 根据指标筛选数据
     *
     * @param metrics metrics
     * @param query   query
     * @return List
     */
    public List<Map<String, Object>> filterMetric(List<Map<String, Object>> metrics, Query query) {
        return CoreUtils.filterMetric(metrics, query.getFilterGroup(), query.getMeasures());
    }

    /**
     * 根据多指标和维度对数据进行排序
     *
     * @param orderBys orderBys // 排序字段列表。字段前加"+|-"标识顺序
     * @param data     data // 数据获取后排序
     */
    public void order(String[] orderBys, List<Map<String, Object>> data) {
        data.sort(new CoreUtils.CubeComparator(orderBys));
    }

}
