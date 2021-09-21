package com.luons.engine.core.utils;

import com.luons.engine.core.filter.Filter;
import com.luons.engine.core.filter.FilterGroup;
import com.luons.engine.core.filter.SimpleFilter;

import java.util.*;

public class CoreUtils {

    public static class CubeComparator implements Comparator<Map<String, Object>> {
        private String[] orderBys;

        public CubeComparator(String[] orderBys) {
            this.orderBys = orderBys;
        }

        @Override
        public int compare(Map<String, Object> o1, Map<String, Object> o2) {
            return compare(o1, o2, orderBys);
        }

        private int compare(Map<String, Object> o1, Map<String, Object> o2, String[] orderBys) {
            if (orderBys == null || orderBys.length == 0) {
                return 0;
            }
            boolean isDesc = orderBys[0].trim().startsWith("-");
            int n = isDesc ? -1 : 1;
            String s = orderBys[0].replaceFirst(("[+\\-]"), ("")).trim();
            Object obj1 = o1.get(s);
            Object obj2 = o2.get(s);
            if ((obj1 == null && obj2 == null)) {
                return compare(o1, o2, Arrays.copyOfRange(orderBys, 1, orderBys.length));
            } else if (obj1 == null) {
                return -1 / n;
            } else if (obj2 == null) {
                return 1 / n;
            } else {
                if (obj1.equals(obj2)) {
                    return compare(o1, o2, Arrays.copyOfRange(orderBys, 1, orderBys.length));
                } else {
                    if (obj1 instanceof String) {
                        return ((String) obj1).compareTo((String) obj2) / n;
                    } else if ((obj1 instanceof Number) && (obj2 instanceof Number)) {
                        Double d1 = ((Number) obj1).doubleValue();
                        Double d2 = ((Number) obj2).doubleValue();
                        return d1.compareTo(d2) / n;
                    } else {
                        return obj1.toString().compareTo(obj2.toString()) / n;
                    }
                }
            }
        }
    }

    /**
     * 根据指标筛选数据
     *
     * @param metrics     数据
     * @param filterGroup 筛选条件
     * @return measures 维度列表(code大写格式)
     */
    public static List<Map<String, Object>> filterMetric(List<Map<String, Object>> metrics, FilterGroup filterGroup, Set<String> measures) {
        if (filterGroup != null && metrics != null & metrics.size() > 0) {
            List<Map<String, Object>> m = new LinkedList<>();
            for (Map<String, Object> data : metrics) {
                if (metricMeetCondition(data, filterGroup, measures)) {
                    m.add(data);
                }
            }
            return m;
        }
        return metrics;
    }


    /**
     * 根据指标筛选判定某条记录是否满足条件
     *
     * @param data
     * @param filterGroup
     * @param measures
     * @return
     */
    private static boolean metricMeetCondition(Map<String, Object> data, FilterGroup filterGroup,
                                               Set<String> measures) {
        if (filterGroup.getFilterList() == null || filterGroup.getFilterList().size() == 0) {
            return true;
        }
        boolean hasMetricFilter = false;
        for (Filter filter : filterGroup.getFilterList()) {
            if (filter instanceof SimpleFilter) {
                SimpleFilter simpleFilter = (SimpleFilter) filter;
                if (measures.contains(simpleFilter.getName())) {
                    hasMetricFilter = true;
                    double fvalue = 0;
                    if (simpleFilter.getValue() instanceof Number) {
                        fvalue = ((Number) simpleFilter.getValue()).doubleValue();
                    } else {
                        throw new IllegalArgumentException(String.format("invalid value type:%s", simpleFilter
                                .getValue().getClass()));
                    }
                    double rvalue = 0;
                    Object obj = data.get(simpleFilter.getName());
                    if (obj != null) {
                        if (obj instanceof Number) {
                            rvalue = ((Number) obj).doubleValue();
                        } else {
                            throw new IllegalArgumentException(String.format("invalid value type:%s",
                                    obj.getClass()));
                        }
                    }
                    switch (filterGroup.getConnector()) {
                        // AND关系时若找到一个false情况则整个记录判定为false
                        case AND:
                            switch (simpleFilter.getOperator()) {
                                case EQ:
                                    if (!(rvalue == fvalue)) {
                                        return false;
                                    }
                                    break;
                                case NE:
                                    if (!(rvalue != fvalue)) {
                                        return false;
                                    }
                                    break;
                                case LT:
                                    if (!(rvalue < fvalue)) {
                                        return false;
                                    }
                                    break;
                                case GT:
                                    if (!(rvalue > fvalue)) {
                                        return false;
                                    }
                                    break;
                                case LE:
                                    if (!(rvalue <= fvalue)) {
                                        return false;
                                    }
                                    break;
                                case GE:
                                    if (!(rvalue >= fvalue)) {
                                        return false;
                                    }
                                    break;
                                default:
                                    throw new IllegalArgumentException(String.format(
                                            "not supported filter operator for metric: %s", simpleFilter.getOperator()
                                                    .getClass().getName()));
                            }
                            break;
                        case OR:
                            // OR关系时若找到一个true情况则整个记录判定为true
                            switch (simpleFilter.getOperator()) {
                                case EQ:
                                    if (rvalue == fvalue) {
                                        return true;
                                    }
                                    break;
                                case NE:
                                    if (rvalue != fvalue) {
                                        return true;
                                    }
                                    break;
                                case LT:
                                    if (rvalue < fvalue) {
                                        return true;
                                    }
                                    break;
                                case GT:
                                    if (rvalue > fvalue) {
                                        return true;
                                    }
                                    break;
                                case LE:
                                    if (rvalue <= fvalue) {
                                        return true;
                                    }
                                    break;
                                case GE:
                                    if (rvalue >= fvalue) {
                                        return true;
                                    }
                                    break;
                                default:
                                    throw new IllegalArgumentException(String.format(
                                            "not supported filter operator for metric: %s", simpleFilter.getOperator()
                                                    .getClass().getName()));
                            }
                            break;
                    }
                }
            } else if (filter instanceof FilterGroup) {
                switch (filterGroup.getConnector()) {
                    case AND:
                        if (!metricMeetCondition(data, (FilterGroup) filter, measures)) {
                            return false;
                        }
                        break;
                    case OR:
                        if (metricMeetCondition(data, (FilterGroup) filter, measures)) {
                            return true;
                        }
                        break;
                }
            }
        }
        switch (filterGroup.getConnector()) {
            case AND:
                return true;
            case OR:
                if (hasMetricFilter) {
                    // 存在指标筛选,但是上边的指标判定中若未能返回true说明所有的指标判定都false,故而返回false
                    return false;
                } else {
                    // 若无指标筛选则返回true
                    return true;
                }
            default:
                return true;
        }
    }

}
