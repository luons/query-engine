package io.github.luons.engine.cube.utils;

import com.google.common.collect.Sets;
import io.github.luons.engine.core.cube.CubeMap;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

public class MergeUtils {

    /**
     * 合并结果集
     *
     * @param dataList1 List<Map<String,Object> 结果集1
     * @param dataList2 List<Map<String,Object> 结果集2
     * @param keyList   合并时的key
     * @return
     */
    public static List<Map<String, Object>> merge(List<Map<String, Object>> dataList1,
                                                  List<Map<String, Object>> dataList2, Set<String> keyList) {
        if (dataList1 == null && dataList2 == null) {
            return new ArrayList<>();
        }
        if (dataList1 == null) {
            return dataList2;
        }
        if (dataList2 == null) {
            return dataList1;
        }
        List<Map<String, Object>> res = new LinkedList<>();
        for (Map<String, Object> itm : dataList1) {
            if (itm == null) {
                continue;
            }
            res.add(new LinkedHashMap<>(itm));
        }
        Map<String, Map<String, Object>> mp = toMap(res, keyList);
        for (Map<String, Object> itm : dataList2) {
            if (itm == null) {
                continue;
            }
            String key = getKey(itm, keyList);
            Map<String, Object> en = mp.get(key);
            if (en == null) {
                res.add(itm);
                continue;
            }
            for (String k : Sets.union(en.keySet(), itm.keySet())) {
                if (keyList.contains(k)) {
                    continue;
                }
                Object o1 = en.get(k);
                Object o2 = itm.get(k);
                if (o1 == null) {
                    en.put(k, o2);
                } else if (o2 == null) {
                    en.put(k, o1);
                } else {
                    if (o1 instanceof Number && o2 instanceof Number) {
                        en.put(k, ((Number) o1).doubleValue() + ((Number) o2).doubleValue());
                    } else {
                        throw new RuntimeException("unsupported data type" + o1 + ":" + o2);
                    }
                }
            }
        }
        return res;
    }


    public static List<CubeMap<Object>> mergeCubeMapList(List<CubeMap<Object>> targetList,
                                                         List<CubeMap<Object>> sourceList, Set<String> keyList) {
        keyList = keyList.stream().map(String::toUpperCase).collect(Collectors.toSet());
        if (targetList == null && sourceList == null) {
            return new ArrayList<>();
        }
        if (targetList == null) {
            return sourceList;
        }
        if (sourceList == null) {
            return targetList;
        }
        List<CubeMap<Object>> res = new LinkedList<>();
        for (Map<String, Object> itm : targetList) {
            if (itm == null) {
                continue;
            }
            CubeMap<Object> cubeMap = new CubeMap<>();
            cubeMap.putAll(itm);
            res.add(cubeMap);

        }
        Map<String, CubeMap<Object>> mp = cubeMapListToMap(res, keyList);
        for (CubeMap<Object> itm : sourceList) {
            if (itm == null) {
                continue;
            }
            String key = getKey(itm, keyList);
            CubeMap<Object> en = mp.get(key);
            if (en == null) {
                res.add(itm);
                continue;
            }
            for (String k : Sets.union(en.keySet(), itm.keySet())) {
                // 若为合并key字段则不需要进行数值合并
                if (keyList.contains(k)) {
                    continue;
                }
                Object o1 = en.get(k);
                Object o2 = itm.get(k);
                if (o1 == null) {
                    en.put(k, o2);
                } else if (o2 != null) {
                    if (o1 instanceof Number && o2 instanceof Number) {
                        en.put(k, ((Number) o1).doubleValue() + ((Number) o2).doubleValue());
                    } else {
                        throw new RuntimeException("unsupported data type" + o1 + ":" + o2);
                    }
                }
            }
        }
        return res;
    }

    public static List<CubeMap<Object>> mergeCubeMapList(Set<String> keyList, List<List<CubeMap<Object>>> tmp) {
        if (tmp == null || tmp.size() == 0) {
            return null;
        } else if (tmp.size() == 1) {
            return tmp.get(0);
        } else {
            List<CubeMap<Object>> res = new LinkedList<>();
            for (List<CubeMap<Object>> cubeMaps : tmp) {
                res = mergeCubeMapList(res, cubeMaps, keyList);
            }
            return res;
        }
    }

    private static Map<String, Map<String, Object>> toMap(List<Map<String, Object>> dataList, Set<String> keyList) {
        Map<String, Map<String, Object>> mp = new LinkedHashMap<>();
        if (CollectionUtils.isEmpty(dataList)) {
            return mp;
        }
        for (Map<String, Object> itm : dataList) {
            if (itm != null) {
                mp.put(getKey(itm, keyList), itm);
            }
        }
        return mp;
    }

    private static Map<String, CubeMap<Object>> cubeMapListToMap(List<CubeMap<Object>> dataList, Set<String> keyList) {
        Map<String, CubeMap<Object>> mp = new LinkedHashMap<>();
        if (CollectionUtils.isEmpty(dataList)) {
            return mp;
        }
        for (CubeMap<Object> itm : dataList) {
            if (itm != null) {
                mp.put(getKey(itm, keyList), itm);
            }
        }
        return mp;
    }

    private static String getKey(Map<String, Object> data, Set<String> keyList) {
        StringBuilder stringBuilder = new StringBuilder();
        if (keyList == null) {
            return stringBuilder.toString();
        }
        int n = 0;
        for (String key : keyList) {
            if (n > 0) {
                stringBuilder.append("#");
            }
            stringBuilder.append(data.get(key));
            n++;
        }
        return stringBuilder.toString();
    }

}
