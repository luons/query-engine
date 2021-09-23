package io.github.luons.engine.cube;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.github.luons.engine.common.ServiceException;
import io.github.luons.engine.core.cube.AbstractCube;
import io.github.luons.engine.core.cube.CubeMap;
import io.github.luons.engine.core.cube.ICube;
import io.github.luons.engine.core.spi.Dimension;
import io.github.luons.engine.core.spi.Query;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class CubeUtils {

    private static final long TIME_OUT = 10L;

    private static final ObjectMapper OM = new ObjectMapper();

    private static final ExecutorService executorService = Executors.newFixedThreadPool((100));

    /**
     * 并行执行对一个cube的多个查询
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @return 同查询顺序对应的结果集列表
     */
    public static List<List<Map<String, Object>>> multiQuery(ThreadPoolExecutor executor,
                                                             final ICube cube, List<? extends Query> queryList) {
        return multiQuery(executor, cube, queryList, TIME_OUT, TimeUnit.SECONDS);
    }

    /**
     * 并行执行对一个cube的多个查询
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @param timeOut   单次查询超时时间
     * @param timeUnit  超时时间单位
     * @return 同查询顺序对应的结果集列表
     */
    public static List<List<Map<String, Object>>> multiQuery(ThreadPoolExecutor executor,
                                                             final ICube cube, List<? extends Query> queryList,
                                                             long timeOut, TimeUnit timeUnit) {
        Preconditions.checkNotNull(queryList, "query list can't be null");
        // 并行查询
        List<List<Map<String, Object>>> res = new LinkedList<>();
        List<Future<List<Map<String, Object>>>> futureList = new LinkedList<>();
        for (final Query query : queryList) {
            Future<List<Map<String, Object>>> future = executor.submit(() -> cube.query(query));
            futureList.add(future);
        }
        try {
            for (Future<List<Map<String, Object>>> f : futureList) {
                res.add(f.get(timeOut, timeUnit));
            }
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            if (e.getCause() instanceof ServiceException) {
                throw (ServiceException) e.getCause();
            }
            throw new RuntimeException(e);
        }
        return res;
    }

    /**
     * 并行执行对多个cube的查询
     *
     * @param executor thread pool
     * @param queryMap 查询列表
     * @return 同查询对应的结果集列表
     */
    public static Map<ICube, List<Map<String, Object>>> multiQuery(ThreadPoolExecutor executor,
                                                                   Map<ICube, Query> queryMap) {
        return multiQuery(executor, queryMap, TIME_OUT, TimeUnit.SECONDS);
    }

    /**
     * 并行执行对多个cube的查询
     *
     * @param executor thread pool
     * @param queryMap 查询列表
     * @param timeOut  单次查询超时时间
     * @param timeUnit 超时时间单位
     * @return 同查询对应的结果集列表
     */
    public static Map<ICube, List<Map<String, Object>>> multiQuery(ThreadPoolExecutor executor,
                                                                   Map<ICube, Query> queryMap,
                                                                   long timeOut, TimeUnit timeUnit) {
        Preconditions.checkNotNull(queryMap, "query can't be null");
        // 并行查询
        Map<ICube, List<Map<String, Object>>> res = new HashMap<>();
        Map<ICube, Future<List<Map<String, Object>>>> cubeFutureMap = new HashMap<>();
        for (final Map.Entry<ICube, Query> en : queryMap.entrySet()) {
            Future<List<Map<String, Object>>> future = executor.submit(() -> en.getKey().query(en.getValue()));
            cubeFutureMap.put(en.getKey(), future);
        }
        try {
            for (Map.Entry<ICube, Future<List<Map<String, Object>>>> en : cubeFutureMap.entrySet()) {
                res.put(en.getKey(), en.getValue().get(timeOut, timeUnit));
            }
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            if (e.getCause() instanceof ServiceException) {
                throw (ServiceException) e.getCause();
            }
            throw new RuntimeException(e);
        }
        return res;
    }

    /**
     * 并行执行对一个cube的多个查询并且进行结果合并
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @return 同查询顺序对应的结果集列表
     */
    public static List<Map<String, Object>> multiQueryAndMerge(ThreadPoolExecutor executor, final AbstractCube cube,
                                                               List<? extends Query> queryList) {
        return multiQueryAndMerge(executor, cube, queryList, TIME_OUT, TimeUnit.SECONDS);
    }

    /**
     * 并行执行对一个cube的多个查询并且进行结果合并、同时返回多个查询的结果详细
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @param timeOut   单次查询超时时间
     * @param timeUnit  超时时间单位
     * @return 同查询顺序对应的结果集列表
     */
    public static List<List<Map<String, Object>>> multiQueryAndMergeV1(ThreadPoolExecutor executor,
                                                                       final AbstractCube cube,
                                                                       List<? extends Query> queryList,
                                                                       long timeOut, TimeUnit timeUnit) {

        Preconditions.checkArgument(!CollectionUtils.isEmpty(queryList), "query list can't be null or empty");
        // 并行查询
        List<List<CubeMap<Object>>> resList = new LinkedList<>();
        List<Future<List<CubeMap<Object>>>> futureList = new LinkedList<>();
        for (final Query query : queryList) {
            Future<List<CubeMap<Object>>> future = executor.submit(() -> cube.rawQuery(query));
            futureList.add(future);
        }
        List<List<CubeMap<Object>>> rs = new LinkedList<>();
        List<CubeMap<Object>> mergeRes = new LinkedList<>();
        try {
            for (Future<List<CubeMap<Object>>> f : futureList) {
                resList.add(f.get(timeOut, timeUnit));
            }
            //计算所有查询合并值
            for (int i = 0; i < resList.size(); i++) {
                Set<String> keyList = getQueryDimensionColumns(cube, queryList.get(0));
                mergeRes = mergeCubeMapList(mergeRes, resList.get(i), keyList);
            }
            if (mergeRes.size() == 0) {
                CubeMap<Object> cubeMap = new CubeMap();
                mergeRes.add(cubeMap);
            }
            rs.add(mergeRes);
            //将单个查询合并作为返回一部分
            for (int i = 0; i < resList.size(); i++) {
                if (resList.get(i).size() > 0) {
                    rs.add(resList.get(i));
                } else {
                    List<CubeMap<Object>> res = new LinkedList<>();
                    CubeMap<Object> cubeMap = new CubeMap();
                    res.add(cubeMap);
                    rs.add(res);
                }
            }
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            if (e.getCause() instanceof ServiceException) {
                throw (ServiceException) e.getCause();
            }
            throw new RuntimeException(e);
        }

        List<List<Map<String, Object>>> metrics = new LinkedList<>();
        for (List<CubeMap<Object>> r : rs) {
            List<Map<String, Object>> metric = cube.filterMetric(
                    cube.toMetric(r, queryList.get(0)), queryList.get(0));
            if (!queryList.get(0).getOrders().isEmpty()) {
                cube.order(queryList.get(0).getOrders().toArray(new String[]{}), metric);
            }
            metrics.add(metric);
        }
        return metrics;
    }

    /**
     * 并行执行对一个cube的多个查询并且进行结果合并
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @param timeOut   单次查询超时时间
     * @param timeUnit  超时时间单位
     * @return 同查询顺序对应的结果集列表
     */
    public static List<Map<String, Object>> multiQueryAndMerge(ThreadPoolExecutor executor,
                                                               final AbstractCube cube,
                                                               List<? extends Query> queryList,
                                                               long timeOut, TimeUnit timeUnit) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(queryList), "query list can't be null or empty");
        // 并行查询
        List<CubeMap<Object>> resList = multiRawQueryAndMerge(executor, cube, queryList, timeOut, timeUnit);
        List<Map<String, Object>> metrics = cube.filterMetric(
                cube.toMetric(resList, queryList.get(0)), queryList.get(0));
        if (!queryList.get(0).getOrders().isEmpty()) {
            cube.order(queryList.get(0).getOrders().toArray(new String[]{}), metrics);
        }
        return metrics;
    }

    /**
     * 并行执行对一个cube的多个查询并且进行结果合并
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @return 同查询顺序对应的结果集列表
     */
    public static List<CubeMap<Object>> multiRawQueryAndMerge(ThreadPoolExecutor executor,
                                                              final AbstractCube cube,
                                                              List<? extends Query> queryList) {
        return multiRawQueryAndMerge(executor, cube, queryList, TIME_OUT, TimeUnit.SECONDS);
    }

    /**
     * 并行执行对一个cube的多个查询并且进行结果合并
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @param timeOut   单次查询超时时间
     * @param timeUnit  超时时间单位
     * @return 同查询顺序对应的结果集列表
     */
    public static List<CubeMap<Object>> multiRawQueryAndMerge(ThreadPoolExecutor executor,
                                                              final AbstractCube cube,
                                                              List<? extends Query> queryList,
                                                              long timeOut, TimeUnit timeUnit) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(queryList), "query list can't be null or empty");
        List<List<CubeMap<Object>>> resList = new LinkedList<>();
        List<Future<List<CubeMap<Object>>>> futureList = new LinkedList<>();
        for (final Query query : queryList) {
            Future<List<CubeMap<Object>>> future = executor.submit(() -> cube.rawQuery(query));
            futureList.add(future);
        }
        try {
            for (Future<List<CubeMap<Object>>> f : futureList) {
                resList.add(f.get(timeOut, timeUnit));
            }
            List<CubeMap<Object>> res = new LinkedList<>();
            for (int i = 0; i < resList.size(); i++) {
                Set<String> keyList = getQueryDimensionColumns(cube, queryList.get(0));
                res = mergeCubeMapList(res, resList.get(i), keyList);
            }
            return res;
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            if (e.getCause() instanceof ServiceException) {
                throw (ServiceException) e.getCause();
            }
            throw new RuntimeException(e);
        }
    }

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
            return null;
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


    public static List<CubeMap<Object>> mergeCubeMapList(List<CubeMap<Object>> dataList1,
                                                         List<CubeMap<Object>> dataList2, Set<String> keyList) {
        keyList = keyList.stream().map(String::toUpperCase).collect(Collectors.toSet());
        if (dataList1 == null && dataList2 == null) {
            return null;
        }
        if (dataList1 == null) {
            return dataList2;
        }
        if (dataList2 == null) {
            return dataList1;
        }
        List<CubeMap<Object>> res = new LinkedList<>();
        for (Map<String, Object> itm : dataList1) {
            if (itm == null) {
                continue;
            }
            CubeMap<Object> cubeMap = new CubeMap<>();
            cubeMap.putAll(itm);
            res.add(cubeMap);

        }
        Map<String, CubeMap<Object>> mp = cubeMapListToMap(res, keyList);
        for (CubeMap<Object> itm : dataList2) {
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
                if (keyList.contains(k)) {
                    // 若为合并key字段则不需要进行数值合并
                    continue;
                }
                Object o1 = en.get(k);
                Object o2 = itm.get(k);
                if (o1 == null) {
                    en.put(k, o2);
                } else if (o2 == null) {
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

    /**
     * 获取一个查询中维度对应的columns
     *
     * @param query query
     * @return Set
     */
    public static Set<String> getQueryDimensionColumns(AbstractCube cube, Query query) {
        Set<String> cols = new LinkedHashSet<>();
        if (query.getDimensions() == null) {
            return cols;
        }
        for (String d : query.getDimensions()) {
            Dimension dim = cube.getDimensions().get(d);
            if (dim == null) {
                throw new IllegalArgumentException(String.format("no such dimension: %s", d));
            } else {
                cols.add(dim.getColumn().getAlias());
            }
        }
        return cols;
    }

    public static List<CubeMap<Object>> multiRawQueryAndMergeV2(ThreadPoolExecutor executor,
                                                                final AbstractCube cube, List<List<Query>> queryCube) {
        return multiRawQueryAndMergeV2(executor, cube, queryCube, TIME_OUT, TimeUnit.SECONDS);
    }

    public static List<CubeMap<Object>> multiRawQueryAndMergeV2(ThreadPoolExecutor executor, final AbstractCube cube,
                                                                List<List<Query>> queryCube,
                                                                long timeOut, TimeUnit timeUnit) {
        Preconditions.checkArgument(!CollectionUtils.isEmpty(queryCube), "query list can't be null or empty");
        // 并行查询
        List<List<Future<List<CubeMap<Object>>>>> futureCube = new LinkedList<>();
        for (List<Query> queryList : queryCube) {
            List<Future<List<CubeMap<Object>>>> futureList = new LinkedList<>();
            for (final Query query : queryList) {
                Future<List<CubeMap<Object>>> future = executor.submit(() -> cube.rawQuery(query));
                futureList.add(future);
            }
            futureCube.add(futureList);
        }
        try {
            List<List<CubeMap<Object>>> resList = new LinkedList<>();
            for (List<Future<List<CubeMap<Object>>>> futureList : futureCube) {
                Set<String> keyList = getQueryDimensionColumns(cube, queryCube.get(0).get(0));
                List<List<CubeMap<Object>>> tmp = new LinkedList<>();
                for (Future<List<CubeMap<Object>>> f : futureList) {
                    tmp.add(f.get(timeOut, timeUnit));
                }
                resList.add(mergeCubeMapList(keyList, tmp));
            }
            List<CubeMap<Object>> res = new LinkedList<>();
            for (int i = 0; i < resList.size(); i++) {
                Set<String> keyList = getQueryDimensionColumns(cube, queryCube.get(0).get(0));
                res = mergeCubeMapList(res, resList.get(i), keyList);
            }
            return res;
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            if (e.getCause() instanceof ServiceException) {
                throw (ServiceException) e.getCause();
            }
            throw new RuntimeException(e);
        }
    }

    public static List<List<Map<String, Object>>> multiQueryV2(ThreadPoolExecutor executor,
                                                               final AbstractCube cube, List<List<Query>> queryCube) {
        return multiQueryV2(executor, cube, queryCube, TIME_OUT, TimeUnit.SECONDS);
    }

    public static List<List<Map<String, Object>>> multiQueryV2(final ThreadPoolExecutor executor,
                                                               final AbstractCube cube, List<List<Query>> queryCube,
                                                               final long timeOut, final TimeUnit timeUnit) {
        Preconditions.checkNotNull(queryCube, "query list can't be null");
        List<List<Map<String, Object>>> res = new LinkedList<>();
        List<Future<List<Map<String, Object>>>> fList = new LinkedList<>();
        for (final List<? extends Query> queryList : queryCube) {
            Future<List<Map<String, Object>>> f = executorService.submit(
                    () -> multiQueryAndMerge(executor, cube, queryList, timeOut, timeUnit));
            fList.add(f);
        }
        for (Future<List<Map<String, Object>>> f : fList) {
            try {
                res.add(f.get(timeOut, timeUnit));
            } catch (ServiceException se) {
                throw se;
            } catch (Exception e) {
                if (e.getCause() instanceof ServiceException) {
                    throw (ServiceException) e.getCause();
                }
                throw new RuntimeException(e);
            }
        }
        /*for (final List<? extends Query> queryList : queryCube) {
            long t = System.currentTimeMillis();
            res.add(multiQueryAndMerge(threadPoolExecutor, cube, queryList, timeOut, timeUnit));
            if (System.currentTimeMillis() - t > 10000) {
                try {
                    log.warn(OM.writeValueAsString(queryList));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }*/
        return res;
    }

    public static List<List<Map<String, Object>>> multiQueryV3(final ThreadPoolExecutor executor,
                                                               final AbstractCube cube,
                                                               List<List<Query>> queryCube,
                                                               final long timeOut, final TimeUnit timeUnit) {
        Preconditions.checkNotNull(queryCube, "query list can't be null");
        List<List<List<Map<String, Object>>>> res = new LinkedList<>();
        List<Future<List<List<Map<String, Object>>>>> fList = new LinkedList<>();
        for (final List<? extends Query> queryList : queryCube) {
            Future<List<List<Map<String, Object>>>> f = executorService.submit(
                    () -> multiQueryAndMergeV1(executor, cube, queryList, timeOut, timeUnit));
            fList.add(f);
        }
        for (Future<List<List<Map<String, Object>>>> f : fList) {
            try {
                res.add(f.get(timeOut, timeUnit));
            } catch (ServiceException se) {
                throw se;
            } catch (Exception e) {
                if (e.getCause() instanceof ServiceException) {
                    throw (ServiceException) e.getCause();
                }
                throw new RuntimeException(e);
            }
        }
        return res.get(0);
    }

    private static List<CubeMap<Object>> mergeCubeMapList(Set<String> keyList, List<List<CubeMap<Object>>> tmp) {
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

    @Data
    public static class DBQuery {

        private String sql;

        private Map<String, Object> param;

        public DBQuery(String sql, Map<String, Object> param) {
            this.sql = sql;
            this.param = param;
        }
    }
}
