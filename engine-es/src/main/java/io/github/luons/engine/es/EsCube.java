package io.github.luons.engine.es;

import com.google.common.base.Preconditions;
import io.github.luons.engine.core.cube.AbstractSqlCube;
import io.github.luons.engine.core.cube.CubeMap;
import io.github.luons.engine.core.enums.Connector;
import io.github.luons.engine.core.enums.Operator;
import io.github.luons.engine.core.filter.Filter;
import io.github.luons.engine.core.filter.FilterGroup;
import io.github.luons.engine.core.filter.SimpleFilter;
import io.github.luons.engine.core.spi.Query;
import io.github.luons.engine.utils.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.NumberFormat;
import java.util.*;

@Slf4j
public class EsCube extends AbstractSqlCube {

    @Autowired
    private EsClient esClient;

    private final String index;

    public EsCube(String index) {
        this.index = index;
        Preconditions.checkNotNull(index);
    }

    @Override
    protected List<CubeMap<Object>> queryDB(Query query) {
        List<Map<String, Object>> dataMapList;
        List<CubeMap<Object>> cubeMapList = new LinkedList<>();
        LinkedHashSet<String> dimensions = query.getDimensions();
        try {
            if (dimensions != null && dimensions.size() > 0) {
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
            item.putAll(map);
            cubeMapList.add(item);
        }
        final LinkedHashSet<String> orderSet = query.getOrders();
        if (orderSet == null) {
            return cubeMapList;
        }
        cubeMapList.sort(new Comparator<CubeMap<Object>>() {
            int sortValue = 0;

            @Override
            public int compare(CubeMap<Object> o1, CubeMap<Object> o2) {
                for (String orderField : orderSet) {
                    final String field = orderField.replaceFirst(("[+\\-]"), ("")).trim();
                    if (!o1.containsKey(field) || !o2.containsKey(field)
                            || o1.get(field) == null || o2.get(field) == null) {
                        continue;
                    }
                    Object v1 = o1.getOrDefault(field, (0));
                    Object v2 = o2.getOrDefault(field, (0));
                    String str1 = v1.toString();
                    String str2 = v2.toString();
                    if (v1 instanceof Number && v2 instanceof Number) {
                        int maxLen = Math.max(v1.toString().length(), v2.toString().length());
                        str1 = addZero2Str((Number) v1, maxLen);
                        str2 = addZero2Str((Number) v2, maxLen);
                    }
                    sortValue = str1.compareTo(str2);
                    if (orderField.contains("-")) {
                        sortValue = -sortValue;
                    }
                    if (0 != sortValue) {
                        break;
                    }
                }
                return sortValue;
            }
        });
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
        Map<String, Object> dslObject = queryToDsl(query);
        LinkedHashSet<String> dimensions = query.getDimensions();
        if (dimensions == null || dimensions.isEmpty()) {
            log.error("{} dimensions isEmpty！", query);
            return new ArrayList<>();
        }
        Map<String, Object> aggMap = new HashMap<>();
        List<String> dimensionList = new ArrayList<>(dimensions);
        for (int i = (dimensionList.size() - 1); i >= 0; i--) {
            String dimension = dimensionList.get(i);
            Map<String, Object> tmpMap = getAggMap(dimension);
            Object dimObj = tmpMap.get(dimension);
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
        return esClient.queryDsl((index + "-*"), "", JacksonUtils.toJson(queryToDsl(query)), scrollMap);
    }

    private static Map<String, Object> queryToDsl(Query query) {
        Map<String, Object> dslObject = new HashMap<>();
        FilterGroup filterGroup = query.getFilterGroup();
        Map<String, Object> queryObject = filterGroupToQueryObject(filterGroup);
        dslObject.put("query", queryObject);
        if (query.getFields() != null && query.getFields().size() > 0) {
            Map<String, Object> includes = new HashMap<>();
            includes.put("includes", query.getFields());
            dslObject.put("_source", includes);
        }
        return dslObject;
    }

    private static Map<String, Object> filterGroupToQueryObject(FilterGroup filterGroup) {
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
                subObject.add(filterToObject((SimpleFilter) filter));
            } else {
                log.error("Unexpected filter object: " + filter.getClass());
            }
        }
        return queryObject;
    }

    private static Map<String, Object> filterToObject(SimpleFilter filter) {
        switch (filter.getOperator()) {
            case EQ: {
                return getEqMap(filter);
            }
            case NE: {
                return addNotMap(getEqMap(filter));
            }
            case LT:
            case GT:
            case LE:
            case GE:
                return getRangeMap(filter);
            case IN:
                return getInMap(filter);
            case NIN:
                return addNotMap(getInMap(filter));
            case EXIST:
                return addExistMap(filter);
        }
        return null;
    }

    private static Map<String, Object> getInMap(SimpleFilter filter) {
        Map<String, Object> re = new HashMap<>();
        Map<String, Object> boolMap = new HashMap<>();
        List<Map<String, Object>> shouldMap = new LinkedList<>();
        boolMap.put("should", shouldMap);
        re.put("bool", boolMap);
        if (!(filter.getValue() instanceof Iterable)) {
            return re;
        }
        for (Object values : (Iterable<?>) filter.getValue()) {
            Map<String, Object> sub = new HashMap<>();
            if (values == null || !values.getClass().isArray()) {
                continue;
            }
            for (Object _v : (Object[]) values) {
                Map<String, Object> _vMap = new HashMap<>();
                _vMap.put(filter.getName(), _v);
                sub.put("match_phrase", _vMap);
                shouldMap.add(sub);
            }
        }
        return re;
    }

    private static Map<String, Object> addNotMap(Map<String, Object> subMap) {
        Map<String, Object> re = new HashMap<>();
        re.put("not", subMap);
        return re;
    }

    private static Map<String, Object> getRangeMap(SimpleFilter filter) {
        String dslOperator = toDslOperator(filter.getOperator());
        Map<String, Object> re = new HashMap<>();
        Map<String, Object> rangeMap = new HashMap<>();
        Map<String, Object> operatorMap = new HashMap<>();
        rangeMap.put(filter.getName(), operatorMap);
        operatorMap.put(dslOperator, filter.getValue());
        re.put("range", rangeMap);
        return re;
    }

    private static Map<String, Object> addExistMap(SimpleFilter filter) {
        Map<String, Object> re = new HashMap<>();
        Map<String, Object> existMap = new HashMap<>();
        existMap.put("field", filter.getName());
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

    private static Map<String, Object> getEqMap(SimpleFilter filter) {
        Map<String, Object> re = new HashMap<>();
        Map<String, Object> termMap = new HashMap<>();
        termMap.put(filter.getName(), filter.getValue());
        re.put("term", termMap);
        return re;
    }

    private Map<String, Object> getAggMap(String field) {
        Map<String, Object> aggMap = new HashMap<>();
        if (StringUtils.isBlank(field)) {
            return aggMap;
        }
        Map<String, Object> fieldMap = new HashMap<>();
        Map<String, Object> term = new HashMap<>();
        term.put("field", field);
        term.put("order", new HashMap<String, String>() {{
            put("_count", "desc");
        }});
        fieldMap.put("terms", term);
        aggMap.put(field, fieldMap);
        return aggMap;
    }

    private static String addZero2Str(Number numObj, int length) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        nf.setMaximumIntegerDigits(length);
        nf.setMinimumIntegerDigits(length);
        return nf.format(numObj);
    }

    public static void main(String[] args) {
        Query query = new Query();
        FilterGroup filterGroup = new FilterGroup();
        filterGroup.addFilter(new SimpleFilter("uniq_id", "12111110"));
        filterGroup.addFilter(new SimpleFilter("time", Operator.GE, 1588852530000L));
        filterGroup.addFilter(new SimpleFilter("time", Operator.LT, 1588852560000L));
        query.setFilterGroup(filterGroup);

        LinkedHashSet<String> set = new LinkedHashSet<>();
        set.add("distinct_id");
        set.add("event");
        query.setFields(set);
        System.out.println(JacksonUtils.toJson(queryToDsl(query)));
    }
}
