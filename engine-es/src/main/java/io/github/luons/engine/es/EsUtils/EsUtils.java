package io.github.luons.engine.es.EsUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class EsUtils {

    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String VALUES = "values";
    private static final String BUCKETS = "buckets";
    private static final String DOC_COUNT = "doc_count";
    private static final String KEY_AS_STRING = "key_as_string";

    public static List<Map<String, Object>> flatMap(List<Map<String, Object>> list) {
        if (list == null || list.size() == 0) {
            return new ArrayList<>();
        }
        List<Map<String, Object>> newList = new ArrayList<>();
        for (Map<String, Object> metricMap : list) {
            if (metricMap == null || metricMap.size() == 0) {
                continue;
            }
            newList.add(getValueRec("", metricMap));
        }
        return newList;
    }

    public static List<Map<String, Object>> results(Map<String, Object> aggregations) {
        List<Map<String, Object>> aggList = new ArrayList<>();
        for (Map.Entry<String, Object> entry : aggregations.entrySet()) {
            aggValues(entry.getValue(), new HashMap<>(), entry.getKey(), aggList);
        }
        return aggList;
    }

    private static List<Map<String, Object>> aggValues(Object agg, Map<String, Object> group, String key,
                                                       List<Map<String, Object>> aggValueList) {
        Map<?, ?> aggMap = (Map<?, ?>) agg;
        if (agg == null || !aggMap.containsKey(BUCKETS) || aggMap.get(BUCKETS) == null) {
            if (aggMap == null) {
                return aggValueList;
            }
            if (aggMap.containsKey(VALUE)) {
                Map<String, Object> dataMap = new HashMap<>();
                dataMap.put(VALUE, aggMap.get(VALUE));
                aggValueList.add(dataMap);
            }
            return aggValueList;
        }
        Object bucketsObject = aggMap.get(BUCKETS);
        if (!(bucketsObject instanceof List)) {
            Map<String, Object> bucketMap = (Map<String, Object>) bucketsObject;
            for (Map.Entry<String, Object> entry : bucketMap.entrySet()) {
                String entryKey = entry.getKey();
                Object entryValue = entry.getValue();
                if (!(entryValue instanceof Map)) {
                    continue;
                }
                Object docCountObject = ((Map<?, ?>) entryValue).get(DOC_COUNT);
                if (Objects.isNull(docCountObject)) {
                    continue;
                }
                Map<String, Object> dataBucket = new HashMap<>();
                dataBucket.put(key, entryKey);
                dataBucket.put("count", docCountObject);
                aggValueList.add(dataBucket);
            }
            return aggValueList;
        }
        for (Map<String, Object> bucketMap : (List<Map<String, Object>>) bucketsObject) {
            String keyAsString = getBucketAggValue(bucketMap);
            group.put(key, keyAsString);
            if (bucketMap.size() == 2 && bucketMap.containsKey(KEY) && bucketMap.containsKey(DOC_COUNT)) {
                LinkedHashMap<String, Object> groupMap = package2map(bucketMap);
                groupMap.putAll(group);
                aggValueList.add(groupMap);
                continue;
            } else if (bucketMap.size() == 3 && bucketMap.containsKey(KEY) && bucketMap.containsKey(DOC_COUNT)
                    && bucketMap.containsKey(KEY_AS_STRING)) {
                LinkedHashMap<String, Object> groupMap = package2map(bucketMap);
                groupMap.putAll(group);
                aggValueList.add(groupMap);
                continue;
            }
            resetAggList(bucketMap, group, aggValueList);
        }
        return aggValueList;
    }

    private static void resetAggList(Map<String, Object> bucketMap, Map<String, Object> group,
                                     List<Map<String, Object>> aggValueList) {

        if (Objects.isNull(bucketMap) || bucketMap.size() == 0) {
            return;
        }
        for (Map.Entry<String, Object> entry : bucketMap.entrySet()) {
            String entryKey = entry.getKey();
            Object entryValue = entry.getValue();
            if (!(entryValue instanceof Map)) {
                continue;
            }
            if (((Map<?, ?>) entryValue).containsKey(VALUE)) {
                LinkedHashMap<String, Object> groupMap = new LinkedHashMap<>(group);
                groupMap.put(VALUE, ((Map<?, ?>) entryValue).get(VALUE));
                aggValueList.add(groupMap);
                break;
            }
            if (((Map<?, ?>) entryValue).containsKey(VALUES)) {
                LinkedHashMap<String, Object> groupMap = new LinkedHashMap<>(group);
                Object valuesObj = ((Map<?, ?>) entryValue).get(VALUES);
                Object tmpObj = null;
                if (valuesObj instanceof List) {
                    for (Object obj : (List<?>) valuesObj) {
                        if (!(obj instanceof Map)) {
                            continue;
                        }
                        tmpObj = ((Map<?, ?>) obj).get(VALUE);
                    }
                } else if (valuesObj instanceof Map) {
                    tmpObj = ((Map<?, ?>) valuesObj).get(VALUE);
                } else {
                    tmpObj = valuesObj;
                }
                groupMap.put(VALUE, tmpObj);
                aggValueList.add(groupMap);
                break;
            }

            for (Map.Entry<String, Object> entry2 : ((Map<String, Object>) entryValue).entrySet()) {
                String entryKey2 = entry2.getKey();
                Object entryValue2 = entry2.getValue();
                if ((entryValue2 instanceof Map) && ((Map<?, ?>) entryValue2).containsKey("lat")
                        && ((Map<?, ?>) entryValue2).containsKey("lon")) {
                    LinkedHashMap<String, Object> location = new LinkedHashMap<>(group);
                    location.putAll((Map<String, Object>) entryValue);
                    aggValueList.add(location);
                    break;
                } else if ((entryValue2 instanceof List) && (BUCKETS).equals(entryKey2)) {
                    if (((List<?>) entryValue2).size() == 0) {
                        aggValueList.add(new LinkedHashMap<>(group));
                        break;
                    }
                    aggValues(entryValue, group, entryKey, aggValueList);
                    break;
                } else if ((entryValue2 instanceof Map) && (BUCKETS).equals(entryKey2)) {
                    for (Map.Entry<String, Object> entry3 : ((Map<String, Object>) entryValue2).entrySet()) {
                        LinkedHashMap<String, Object> fromToMap = new LinkedHashMap<>(group);
                        Object entryValue3 = entry3.getValue();
                        if (entry3.getValue() instanceof Map && ((Map<?, ?>) entryValue3).containsKey(DOC_COUNT)) {
                            fromToMap.put(entryKey, entry3.getKey());
                            fromToMap.put("count", ((Map<?, ?>) entryValue3).get(DOC_COUNT));
                        }
                        aggValueList.add(fromToMap);
                    }
                    break;
                }
            }
            // LinkedHashMap<String, Object> groupMap = package2map(_value);
            // groupMap.putAll(group);
            // aggValueList.add(groupMap);
            // break;
        }
    }

    private static LinkedHashMap<String, Object> package2map(Object bucket) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        if (bucket instanceof Map && ((Map<?, ?>) bucket).containsKey(KEY)) {
            Object obj = ((Map<?, ?>) bucket).get(DOC_COUNT);
            map.put(VALUE, obj);
        }
        return map;
    }

    private static String getBucketAggValue(Map<String, Object> bucketMap) {
        if (bucketMap == null || bucketMap.size() == 0) {
            return "";
        }
        if (bucketMap.containsKey(KEY)) {
            return bucketMap.get(KEY).toString();
        } else if (bucketMap.containsKey(KEY_AS_STRING)) {
            return bucketMap.get(KEY_AS_STRING).toString();
        } else {
            return "";
        }
    }

    private static Map<String, Object> getValueRec(String keyPrefix, Map<String, Object> metricMap) {
        Map<String, Object> newMetric = new HashMap<>();
        if (metricMap == null || metricMap.size() == 0) {
            if (StringUtils.isNotBlank(keyPrefix)) {
                newMetric.put(keyPrefix, null);
            }
            return newMetric;
        }
        for (Map.Entry<String, Object> entry : metricMap.entrySet()) {
            String key = entry.getKey();
            if (StringUtils.isBlank(key)) {
                continue;
            }
            Object value = entry.getValue();
            if (StringUtils.isNotBlank(keyPrefix)) {
                key = keyPrefix + "." + key;
            }
            if (!(value instanceof Map)) {
                newMetric.put(key, value);
                continue;
            }
            Map<String, Object> valueRec = getValueRec(key, (Map<String, Object>) value);
            if (valueRec.size() > 0) {
                newMetric.putAll(valueRec);
                continue;
            }
            newMetric.put(key, null);
        }
        return newMetric;
    }

}
