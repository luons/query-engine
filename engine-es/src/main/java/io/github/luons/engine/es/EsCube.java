package io.github.luons.engine.es;

import com.google.common.base.Preconditions;
import io.github.luons.engine.core.cube.AbstractSqlCube;
import io.github.luons.engine.core.cube.CubeMap;
import io.github.luons.engine.core.enums.Aggregations;
import io.github.luons.engine.core.enums.Connector;
import io.github.luons.engine.core.enums.Operator;
import io.github.luons.engine.core.filter.Filter;
import io.github.luons.engine.core.filter.FilterGroup;
import io.github.luons.engine.core.filter.SimpleFilter;
import io.github.luons.engine.core.spi.Column;
import io.github.luons.engine.core.spi.Dimension;
import io.github.luons.engine.core.spi.Measure;
import io.github.luons.engine.core.spi.Query;
import io.github.luons.engine.utils.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.NumberFormat;
import java.time.ZoneId;
import java.util.*;

@Slf4j
public class EsCube extends AbstractSqlCube {

    @Autowired
    private EsClient esClient;

    private final String index;

    private static int minDocCount = 1;

    public EsCube(String index) {
        this.index = index;
        Preconditions.checkNotNull(index);
    }

    @Override
    protected List<CubeMap<Object>> queryDB(Query query) {
        List<Map<String, Object>> dataMapList;
        List<CubeMap<Object>> cubeMapList = new LinkedList<>();
        LinkedHashSet<String> dimensionKeys = query.getDimensions();
        try {
            if (dimensionKeys != null && dimensionKeys.size() > 0) {
                dataMapList = queryDbByAggs(query);
            } else {
                dataMapList = queryDbByDsl(query);
            }
        } catch (Exception e) {
            log.error("{} is exception " + e, JacksonUtils.toJson(query));
            return cubeMapList;
        }
        if (dataMapList == null || dataMapList.size() == 0) {
            return cubeMapList;
        }
        for (Map<String, Object> map : dataMapList) {
            CubeMap<Object> item = new CubeMap<>();
            Map<String, Object> tmpMap = new HashMap<>(map.size());
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = entry.getKey();
                Dimension dimension = dimensions.get(key);
                if (dimension != null && dimension.getColumn() != null) {
                    key = dimensions.get(key).getColumn().getAlias();
                }
                tmpMap.put(key, entry.getValue());
            }
            item.putAll(tmpMap);
            cubeMapList.add(item);
        }

        aggOrderBy(cubeMapList, query.getOrders());
        return cubeMapList;
    }

    // TODO $需加双引号
    public List<Map<String, Object>> queryDbBySql(Query query) throws Exception {
        Map<String, Object> param = new LinkedHashMap<>();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(queryToSelectSql(query));
        stringBuilder.append(" FROM \"").append(index).append("-*\"");
        String where = queryToWhereSql(query, param);
        if (where != null && where.length() > 0) {
            stringBuilder.append(where);
        }
        String groupBy = queryToGroupBySql(query);
        if (groupBy != null) {
            stringBuilder.append(groupBy);
        }
        stringBuilder.append(" LIMIT 1000");
        String sql = stringBuilder.toString();
        log.info("queryDbBySql sql = {}", sql);
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(("/tmp/" + System.currentTimeMillis()))));
            bw.write(sql);
            bw.close();
        } catch (Exception e) {
            log.error("queryDbBySql BufferedWriter is exception " + e);
        }
        return esClient.querySql(sql);
    }

    private List<Map<String, Object>> queryDbByAggs(Query query) throws Exception {
        Map<String, Object> dslObject = queryToDsl(query, false);
        LinkedHashSet<String> dimSet = query.getDimensions();
        if (dimSet == null || dimSet.isEmpty()) {
            log.error("{} dimensions isEmpty！", query);
            return new ArrayList<>();
        }
        Map<String, Object> aggMap = new HashMap<>();
        List<String> dimensionList = new ArrayList<>(dimSet);
        Map<String, Object> measureMapMap = getMeasureMap(query.getMeasures());
        for (int i = dimensionList.size() - 1; i >= 0; i--) {
            String dimensionKey = dimensionList.get(i);
            if (StringUtils.isBlank(dimensionKey) || dimensions.get(dimensionKey) == null) {
                continue;
            }
            Dimension dimension = dimensions.get(dimensionKey);
            Map<String, Object> tmpMap = getAggMap(dimension, i);
            Object dimObj = tmpMap.get(dimension.getColumn().getAlias());
            if (aggMap.size() == 0 && measureMapMap.size() > 0) {
                ((Map<String, Object>) dimObj).put("aggs", measureMapMap);
            }
            if (aggMap.size() > 0 && !Objects.isNull(dimObj)) {
                ((Map<String, Object>) dimObj).put("aggs", aggMap);
            }
            aggMap = tmpMap;
        }
        dslObject.put("size", 0);
        dslObject.put("aggs", aggMap);

        return esClient.queryDslForAggs((index + "-*"), "", JacksonUtils.toJson(dslObject));
    }

    private List<Map<String, Object>> queryDbByDsl(Query query) throws Exception {
        Map<String, String> scrollMap = new HashMap<>();
        return esClient.queryDsl((index + "-*"), "", JacksonUtils.toJson(queryToDsl(query, true)), scrollMap);
    }

    private Map<String, Object> queryToDsl(Query query, boolean isOrder) {
        Map<String, Object> dslObject = new HashMap<>();
        FilterGroup filterGroup = query.getFilterGroup();
        Map<String, Object> queryObject = filterGroupToQueryObject(filterGroup);
        dslObject.put("query", queryObject);
        if (query.getFields() != null && query.getFields().size() > 0) {
            Map<String, Object> includes = new HashMap<>();
            includes.put("includes", query.getFields());
            dslObject.put("_source", includes);
        }
        // Order
        if (isOrder && query.getOrders() != null && query.getOrders().size() > 0) {
            List<Map<String, Map<String, String>>> sortMapList = new ArrayList<>();
            for (String orderKey : query.getOrders()) {
                Column column = getColumnByKey(orderKey);
                if (column == null) {
                    continue;
                }
                String orderBy = "asc";
                if ("-".equals(orderKey.substring(0, 1))) {
                    orderBy = "desc";
                }
                Map<String, String> orderMap = new HashMap<>();
                orderMap.put("order", orderBy);
                Map<String, Map<String, String>> sortMap = new LinkedHashMap<>();
                sortMap.put(column.getColumn(), orderMap);
                sortMapList.add(sortMap);
            }
            dslObject.put("sort", sortMapList);
        }
        // 默认设置单次查询最大2000条数据
        dslObject.put("size", EsClient.QUERY_COUNT);
        return dslObject;
    }

    private Map<String, Object> filterGroupToQueryObject(FilterGroup filterGroup) {
        Map<String, Object> queryObject = new HashMap<>();
        Connector connector = filterGroup.getConnector();
        Map<String, Object> boolObject = new HashMap<>();
        List<Map<String, Object>> subObject = new LinkedList<>();
        if (Connector.AND.equals(connector)) {
            boolObject.put("must", subObject);
        } else if (Connector.OR.equals(connector)) {
            boolObject.put("should", subObject);
        }
        queryObject.put("bool", boolObject);
        List<Filter> filterList = filterGroup.getFilterList();
        for (Filter filter : filterList) {
            if (filter instanceof FilterGroup) {
                subObject.add(filterGroupToQueryObject((FilterGroup) filter));
            } else if (filter instanceof SimpleFilter) {
                String filterName = ((SimpleFilter) filter).getName().toUpperCase();
                Column column = null;
                if (dimensions.containsKey(filterName)) {
                    column = dimensions.get(filterName).getColumn();
                } else if (measures.containsKey(filterName)) {
                    column = measures.get(filterName).getColumns()[0];
                }
                if (column == null || StringUtils.isBlank(column.getColumn())) {
                    continue;
                }
                Map<String, Object> data = filterToObject((SimpleFilter) filter, column);
                if (data == null || data.size() == 0) {
                    continue;
                }
                subObject.add(data);
            } else {
                log.error("Unexpected filter object: " + filter.getClass());
            }
        }
        return queryObject;
    }

    private static Map<String, Object> filterToObject(SimpleFilter filter, Column column) {

        String columnName = column.getColumn();
        Object columnValue = filter.getValue();
        switch (filter.getOperator()) {
            case EQ: {
                return getEqMap(columnName, columnValue);
            }
            case NE: {
                return addNotMap(getEqMap(columnName, columnValue));
            }
            case LT:
            case GT:
            case LE:
            case GE:
                return getRangeMap(columnName, filter);
            case IN:
                return getInMap(columnName, columnValue);
            case NIN:
                return addNotMap(getInMap(columnName, columnValue));
            case EXIST:
                return addExistMap(columnName);
        }
        return null;
    }

    private static Map<String, Object> getInMap(String columnName, Object values) {
        Map<String, Object> re = new HashMap<>();
        Map<String, Object> boolMap = new HashMap<>();
        List<Map<String, Object>> shouldMap = new LinkedList<>();
        boolMap.put("should", shouldMap);
        re.put("bool", boolMap);
        if (!(values instanceof Iterable)) {
            return new HashMap<>();
        }
        for (Object value : (Iterable<?>) values) {
            Map<String, Object> sub = new HashMap<>();
            Map<String, Object> map = new HashMap<>();
            map.put(columnName, value);
            sub.put("match_phrase", map);
            shouldMap.add(sub);
        }
        return re;
    }

    private static Map<String, Object> addNotMap(Map<String, Object> subMap) {
        if (subMap == null || subMap.size() == 0) {
            return new HashMap<>();
        }
        Map<String, Object> re = new HashMap<>();
        re.put("not", subMap);
        return re;
    }

    private static Map<String, Object> getRangeMap(String columnName, SimpleFilter filter) {
        String dslOperator = toDslOperator(filter.getOperator());
        Map<String, Object> re = new HashMap<>();
        Map<String, Object> rangeMap = new HashMap<>();
        Map<String, Object> operatorMap = new HashMap<>();
        rangeMap.put(columnName, operatorMap);
        operatorMap.put(dslOperator, filter.getValue());
        re.put("range", rangeMap);
        return re;
    }

    private static Map<String, Object> addExistMap(String columnName) {
        Map<String, Object> re = new HashMap<>();
        Map<String, Object> existMap = new HashMap<>();
        existMap.put("field", columnName);
        re.put("exists", existMap);
        return re;
    }

    private static String toDslOperator(Operator operator) {
        switch (operator) {
            case LT:
                return "lt";
            case GT:
                return "gt";
            case LE:
                return "lte";
            case GE:
                return "gte";
            case EXIST:
                return "exists";
            case EQ:
            case NE:
            case IN:
            case NIN:
            default:
                throw new UnsupportedOperationException(
                        "Unsupported operator for range statement: " + operator);
        }
    }

    private static Map<String, Object> getEqMap(String columnName, Object value) {
        Map<String, Object> re = new HashMap<>();
        Map<String, Object> termMap = new HashMap<>();
        termMap.put(columnName, value);
        re.put("term", termMap);
        return re;
    }

    private Map<String, Object> getAggMap(Dimension dimension, int i) {
        Map<String, Object> aggMap = new HashMap<>();
        String fieldName = dimension.getColumn().getColumn();
        String aliasName = dimension.getColumn().getAlias();
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        // TODO 如果有粒度，且时间必须放在首位
        if (StringUtils.isNotBlank(granularity) && i == 0 && fieldName.contains("time")) {
            term.put("field", fieldName);
            term.put("interval", granularity);
            term.put("time_zone", TimeZone.getTimeZone(ZoneId.systemDefault()).getID());
            term.put("min_doc_count", minDocCount);
            fieldMap.put("date_histogram", term);
            aggMap.put(aliasName, fieldMap);
            return aggMap;
        }
        term.put("field", fieldName);
        term.put("order", new HashMap<String, String>() {{
            put("_count", "desc");
        }});
        fieldMap.put("terms", term);
        aggMap.put(aliasName, fieldMap);
        return aggMap;
    }

    private Map<String, Object> getMeasureMap(LinkedHashSet<String> measureKeys) {
        Map<String, Object> measureMapMap = new LinkedHashMap<>();
        if (measureKeys == null || measureKeys.size() == 0) {
            return measureMapMap;
        }
        for (String measureKey : measureKeys) {
            Measure measure = measures.get(measureKey);
            if (Objects.isNull(measure) || measure.getColumns() == null) {
                continue;
            }
            Map<String, Object> measureTmpMap = genColumnMetricMap(measureKey, new ArrayList<>());
            if (Objects.isNull(measureTmpMap)) {
                continue;
            }
            measureMapMap.putAll(measureTmpMap);
        }
        return measureMapMap;
    }

    private Map<String, Object> genColumnMetricMap(String measureAliasKey, List<String> measureKeyList) {
        if (StringUtils.isBlank(measureAliasKey)) {
            return null;
        }
        Measure measure = measures.get(measureAliasKey);
        if (Objects.isNull(measure) || measure.getColumns() == null) {
            return null;
        }
        Map<String, Object> measureMapMap = new LinkedHashMap<>();
        Map<String, Map<String, String>> columnMap = new LinkedHashMap<>();
        for (Column column : measure.getColumns()) {
            String colName = column.getColumn();
            // 判断是否是复合指标
            measureKeyList.add(measureAliasKey);
            String newMeasureAliasKey = getComplexTargetKey(colName, measureKeyList);
            if (StringUtils.isNotBlank(newMeasureAliasKey)) {
                Map<String, Object> measureTmpMap = genColumnMetricMap(newMeasureAliasKey, measureKeyList);
                if (!Objects.isNull(measureTmpMap) && measureTmpMap.size() > 0) {
                    measureMapMap.putAll(measureTmpMap);
                }
            }
            Map<String, String> map = new HashMap<>();
            String key = StringUtils.isNotBlank(column.getAlias()) ? column.getAlias() : colName;
            if (Aggregations.DERIVATIVE.getAgg().equalsIgnoreCase(column.getAggregation().getAgg())
                    && StringUtils.isNotBlank(granularity)) {
                map.put("buckets_path", colName);
                minDocCount = 0;
            } else {
                map.put("field", colName);
            }
            // TODO 注意 Agg 是否适配
            String aggNames = column.getAggregation().name().toLowerCase();
            columnMap.put(aggNames, map);
            measureMapMap.put(key, columnMap);
        }
        return measureMapMap;
    }


    private static String addZero2Str(Number numObj, int length) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        nf.setMaximumIntegerDigits(length);
        nf.setMinimumIntegerDigits(length);
        return nf.format(numObj);
    }

    private void aggOrderBy(List<CubeMap<Object>> cubeMapList, Set<String> orderSet) {
        if (orderSet == null) {
            return;
        }
        cubeMapList.sort(new Comparator<CubeMap<Object>>() {
            int sortValue = 0;

            @Override
            public int compare(CubeMap<Object> o1, CubeMap<Object> o2) {
                for (String orderKey : orderSet) {
                    boolean isDesc = "-".equals(orderKey.substring(0, 1));
                    orderKey = isDesc ? orderKey.substring(1) : orderKey;
                    Column column = getColumnByKey(orderKey);
                    if (column == null) {
                        continue;
                    }
                    String orderField = column.getAlias();
                    if (!o1.containsKey(orderField) || !o2.containsKey(orderField)) {
                        continue;
                    }
                    Object v1 = o1.getOrDefault(orderField, (0));
                    Object v2 = o2.getOrDefault(orderField, (0));
                    String str1 = v1.toString();
                    String str2 = v2.toString();
                    if (v1 instanceof Number && v2 instanceof Number) {
                        int maxLen = Math.max(v1.toString().length(), v2.toString().length());
                        str1 = addZero2Str((Number) v1, maxLen);
                        str2 = addZero2Str((Number) v2, maxLen);
                    }
                    sortValue = str1.compareTo(str2);
                    if (isDesc) {
                        sortValue = -sortValue;
                    }
                    if (0 != sortValue) {
                        break;
                    }
                }
                return sortValue;
            }
        });
    }

    private Column getColumnByKey(String key) {
        Dimension dimension = dimensions.get(key);
        if (dimension != null && dimension.getColumn() != null) {
            return dimension.getColumn();
        }
        Measure measure = measures.get(key);
        if (measure != null && measure.getColumns() != null) {
            return measure.getColumns()[0];
        }
        return null;
    }

    private String getComplexTargetKey(String colName, List<String> measureList) {
        if (measures == null || measures.isEmpty()) {
            return "";
        }
        for (Map.Entry<String, Measure> column : measures.entrySet()) {
            Measure measureCol = column.getValue();
            if (Objects.isNull(measureCol) || Objects.isNull(measureCol.getColumns())) {
                continue;
            }
            if (measureList.contains(measureCol.getCode())) {
                continue;
            }
            for (Column col : measureCol.getColumns()) {
                if (colName.equals(col.getAlias())) {
                    return measureCol.getCode();
                }
            }
        }
        return "";
    }

}
