package io.github.luons.engine.core.cube;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import io.github.luons.engine.core.enums.Operator;
import io.github.luons.engine.core.filter.Filter;
import io.github.luons.engine.core.filter.FilterGroup;
import io.github.luons.engine.core.filter.SimpleFilter;
import io.github.luons.engine.core.spi.Dimension;
import io.github.luons.engine.core.spi.IMeasureMinDimSets;
import io.github.luons.engine.core.spi.Measure;
import io.github.luons.engine.core.spi.Query;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

@Data
public abstract class AbstractSqlCube extends AbstractCube implements IMeasureMinDimSets {

    private Map<String, List<Set<String>>> measureMinDimSetsMap;

    @Override
    public Map<String, List<Set<String>>> getMeasureMinDimSetsMap() {
        return measureMinDimSetsMap;
    }

    @Override
    public void setMeasureMinDimSetsMap(Map<String, List<Set<String>>> measureMinDimSetsMap) {
        this.measureMinDimSetsMap = measureMinDimSetsMap;
    }

    protected abstract List<CubeMap<Object>> queryDB(Query query);

    /**
     * cube search,返回结果为维度指标涉及到列的列表
     */
    @Override
    public List<CubeMap<Object>> rawQuery(Query query) {
        validateQuery(query);
        if (!Objects.isNull(limitFilterGroup)) {
            FilterGroup filterGroup = new FilterGroup()
                    .addFilter(query.getFilterGroup())
                    .addFilter(limitFilterGroup);
            query.filterGroup(filterGroup);
        }
        return queryDB(query);
    }

    /**
     * 查询cube数据,返回结果为根据维度指标进行计算和排序的列表
     */
    @Override
    public List<Map<String, Object>> query(Query query) {
        List<CubeMap<Object>> rawMetrics = rawQuery(query);
        if ("ES".equals(cubeName)) {
            List<Map<String, Object>> metrics = new ArrayList<>();
            for (CubeMap<Object> cubeMap : rawMetrics) {
                metrics.add(new HashMap<>(cubeMap));
            }
            return metrics;
        }
        // if (!query.getOrders().isEmpty()) {
        //     order(query.getOrders().toArray(new String[]{}), metrics);
        // }
        return filterMetric(toMetric(rawMetrics, query), query);
    }

    protected String queryToSelectSql(Query query) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SELECT ");
        if (query.getHints() != null && query.getHints().size() > 0) {
            stringBuilder.append(" /*+ ");
            stringBuilder.append(Joiner.on(",").join(query.getHints()));
            stringBuilder.append(" */");
        }
        int n = 0;
        if (query.getDimensions() != null) {
            for (String d : query.getDimensions()) {
                Dimension dimension = dimensions.get(d);
                if (n > 0) {
                    stringBuilder.append(",");
                }
                stringBuilder.append(" ").append(dimension.getColumn().getColumn());
                stringBuilder.append(" AS ").append(dimension.getColumn().getAlias());
                n++;
            }
        }
        if (query.getRangeDims() != null && !query.getRangeDims().isEmpty()) {
            for (Query.RangeDim rangeDim : query.getRangeDims()) {
                String dimCode = rangeDim.getDimCode();
                List<Object> splits = rangeDim.getSplits();
                if (n > 0) {
                    stringBuilder.append(",");
                }
                Dimension dimension = dimensions.get(dimCode);
                stringBuilder.append(" ").append(toCaseStatement(dimension.getColumn().getColumn(), splits));
                stringBuilder.append(" AS ").append(dimension.getColumn().getAlias());
                n++;
            }
        }
        if (query.getMeasures() != null) {
            for (String measureKey : query.getMeasures()) {
                Measure measure = measures.get(measureKey);
                if (n > 0) {
                    stringBuilder.append(",");
                }
                for (int i = 0; i < measure.getColumns().length; i++) {
                    if (i > 0) {
                        stringBuilder.append(",");
                    }
                    if (measure.getColumns()[i].isAggr()) {
                        // 聚合列不需要sum
                        stringBuilder.append(measure.getColumns()[i].getColumn()).append(" AS ");
                        stringBuilder.append(measure.getColumns()[i].getAlias());
                    } else {
                        // 非聚合列添加sum
                        stringBuilder.append(" SUM(").append(measure.getColumns()[i].getColumn());
                        stringBuilder.append(") AS ").append(measure.getColumns()[i].getAlias());
                    }
                }
                n++;
            }
        }
        if (query.getFields() != null) {
            for (String field : query.getFields()) {
                if (n > 0) {
                    stringBuilder.append(", ");
                }
                stringBuilder.append(field);
                n++;
            }
        }
        return stringBuilder.toString();
    }

    protected String queryToOrderBy(Query query) {
        if (query.getOrders().isEmpty()) {
            return "";
        }
        LinkedHashSet<String> orders = query.getOrders();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(" ORDER BY ");
        int i = 0;
        for (String field : orders) {
            if (StringUtils.isBlank(field)) {
                continue;
            }
            if (i > 0) {
                stringBuilder.append(", ");
            }
            if (field.contains("-")) {
                stringBuilder.append(field.replaceFirst(("[+\\-]"), ("")).trim());
                stringBuilder.append(" DESC");
            } else {
                stringBuilder.append(field.replaceFirst(("[+\\-]"), ("")).trim());
                stringBuilder.append(" ASC");
            }
            i++;
        }
        return stringBuilder.toString();
    }

    protected String queryToWhereSql(Query query, Map<String, Object> param) {
        if (query.getFilterGroup() == null || query.getFilterGroup().getFilterList() == null
                || query.getFilterGroup().getFilterList().size() == 0) {
            return null;
        }
        return " WHERE " + filterGroupToSql(query.getFilterGroup(), param);
    }

    protected String queryToGroupBySql(Query query) {
        // TODO 判断是否需要进行指标聚合
        if (Objects.isNull(query) || (query.isEmpty(query.getDimensions()) && query.isEmpty(query.getRangeDims()))) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(" GROUP BY");
        int n = 0;
        if (query.getDimensions() != null) {
            for (String d : query.getDimensions()) {
                Dimension dimension = dimensions.get(d);
                if (n > 0) {
                    stringBuilder.append(",");
                }
                stringBuilder.append(" ").append(dimension.getColumn().getColumn());
                n++;
            }
        }
        if (query.getRangeDims() != null && !query.getRangeDims().isEmpty()) {
            if (query.getRangeDims() != null && !query.getRangeDims().isEmpty()) {
                for (Query.RangeDim rangeDim : query.getRangeDims()) {
                    String dimCode = rangeDim.getDimCode();
                    List<Object> splits = rangeDim.getSplits();
                    if (n > 0) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append(" ");
                    Dimension dimension = dimensions.get(dimCode);
                    stringBuilder.append(toCaseStatement(dimension.getColumn().getColumn(), splits));
                    n++;
                }
            }
        }
        return stringBuilder.toString();
    }

    /**
     * 将filterGroup转换为对应的SQL
     *
     * @param filterGroup // filterGroup
     * @param param       查询时对应的参数
     */
    protected String filterGroupToSql(FilterGroup filterGroup, Map<String, Object> param) {
        Preconditions.checkNotNull(filterGroup);
        Preconditions.checkNotNull(param);
        Preconditions.checkNotNull(filterGroup.getFilterList());
        Preconditions.checkArgument((filterGroup.getFilterList().size() > 0));
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(" (");
        for (int i = 0; i < filterGroup.getFilterList().size(); i++) {
            Filter f = filterGroup.getFilterList().get(i);
            if (i > 0) {
                stringBuilder.append(" ").append(filterGroup.getConnector().name());
            }
            if (f instanceof SimpleFilter) {
                setQueryBySimpleFilter(f, param, stringBuilder);
            } else if (f instanceof FilterGroup) {
                FilterGroup group = (FilterGroup) f;
                if (group.getFilterList() == null || group.getFilterList().size() > 0) {
                    stringBuilder.append(filterGroupToSql(group, param));
                } else {
                    switch (filterGroup.getConnector()) {
                        case OR:
                            stringBuilder.append(" (1>1) ");
                            break;
                        case AND:
                            stringBuilder.append(" (1=1) ");
                            break;
                        default:
                            break;
                    }
                }
            } else {
                throw new IllegalArgumentException("invalid filter type");
            }
        }
        stringBuilder.append(" ) ");
        return stringBuilder.toString();
    }

    private static String toCaseStatement(String column, List<Object> splits) {
        StringBuilder sb = new StringBuilder(" CASE ");
        int i = 0;
        while (i < splits.size()) {
            sb.append(" WHEN ").append(column).append(" < ").append(splits.get(i)).append(" THEN ").append(i);
            i++;
        }
        sb.append(" ELSE ").append(i).append(" END ");
        return sb.toString();
    }

    private void setQueryBySimpleFilter(Filter f, Map<String, Object> param, StringBuilder stringBuilder) {
        SimpleFilter simpleFilter = (SimpleFilter) f;
        Measure measure = measures.get(simpleFilter.getName());
        if (measure != null) {
            stringBuilder.append(" 1=1 ");
            return;
        }
        Dimension dimension = dimensions.get(simpleFilter.getName());
        if (dimension != null) {
            stringBuilder.append(" ").append(dimension.getColumn().getColumn());
        } else {
            // limits中可能存在非维度的过滤
            stringBuilder.append(" ").append(simpleFilter.getName());
        }
        switch (simpleFilter.getOperator()) {
            case Operator.EQ:
                stringBuilder.append(Operator.EQ.getExpInSql());
                break;
            case Operator.NE:
                stringBuilder.append(Operator.NE.getExpInSql());
                break;
            case Operator.LT:
                stringBuilder.append(Operator.LT.getExpInSql());
                break;
            case Operator.GT:
                stringBuilder.append(Operator.GT.getExpInSql());
                break;
            case Operator.LE:
                stringBuilder.append(Operator.LE.getExpInSql());
                break;
            case Operator.GE:
                stringBuilder.append(Operator.GE.getExpInSql());
                break;
            case Operator.IN:
                stringBuilder.append(Operator.IN.getExpInSql());
                break;
            case Operator.NIN:
                stringBuilder.append(Operator.NIN.getExpInSql());
                break;
            case Operator.LIKE:
                stringBuilder.append(Operator.LIKE.getExpInSql());
                break;
        }
        switch (simpleFilter.getOperator()) {
            case Operator.IN:
            case Operator.NIN:
                if (!(simpleFilter.getValue() instanceof Object[] || simpleFilter.getValue() instanceof List)) {
                    throw new IllegalArgumentException("param type for IN and NIN must be Object[] or List");
                } else {
                    stringBuilder.append(" (");
                    int n = 0;
                    Object[] vList;
                    if (simpleFilter.getValue() instanceof List) {
                        vList = ((List) simpleFilter.getValue()).toArray();
                    } else {
                        vList = (Object[]) simpleFilter.getValue();
                    }
                    for (Object obj : vList) {
                        if (n > 0) {
                            stringBuilder.append(",");
                        }
                        param.put("v" + (param.size() + 1), obj);
                        stringBuilder.append(" #{param.v").append(param.size()).append("}");
                        n++;
                    }
                    stringBuilder.append(" )");
                }
                break;
            default:
                param.put("v" + (param.size() + 1), simpleFilter.getValue());
                stringBuilder.append(" #{param.v").append(param.size()).append("}");
        }
    }

}
