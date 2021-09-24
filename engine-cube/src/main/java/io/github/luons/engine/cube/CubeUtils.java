package io.github.luons.engine.cube;

import com.google.common.base.Preconditions;
import io.github.luons.engine.common.ServiceException;
import io.github.luons.engine.core.cube.AbstractCube;
import io.github.luons.engine.core.cube.CubeMap;
import io.github.luons.engine.core.cube.ICube;
import io.github.luons.engine.core.spi.Dimension;
import io.github.luons.engine.core.spi.Query;
import io.github.luons.engine.cube.utils.MergeUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class CubeUtils {

    private static final long TIME_OUT_MS = 10 * 1000L;

    private static final ExecutorService EXECUTOR_POOL = Executors.newFixedThreadPool((100));

    /**
     * 并行执行对一个cube的多个查询
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @return 同查询顺序对应的结果集列表
     */
    public static List<List<Map<String, Object>>> multiQuery(ThreadPoolExecutor executor, final ICube cube,
                                                             List<? extends Query> queryList) {
        return multiQuery(executor, cube, queryList, TIME_OUT_MS);
    }

    /**
     * 并行执行对一个cube的多个查询
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @param timeOutMs 单次查询超时时间
     * @return 同查询顺序对应的结果集列表
     */
    public static List<List<Map<String, Object>>> multiQuery(ThreadPoolExecutor executor, final ICube cube,
                                                             List<? extends Query> queryList, long timeOutMs) {
        checkCubeNotNull(queryList);
        List<Future<List<Map<String, Object>>>> futureList = queryList.stream()
                .map(query -> executor.submit(() -> cube.query(query)))
                .collect(Collectors.toCollection(LinkedList::new));
        return futures(timeOutMs, futureList);
    }

    /**
     * 并行执行对多个cube的查询
     *
     * @param executor executor
     * @param map      查询列表
     * @return 同查询对应的结果集列表
     */
    public static Map<ICube, List<Map<String, Object>>> multiQuery(ThreadPoolExecutor executor, Map<ICube, Query> map) {
        return multiQuery(executor, map, TIME_OUT_MS);
    }

    /**
     * 并行执行对多个cube的查询
     *
     * @param executor  thread pool
     * @param queryMap  查询列表
     * @param timeOutMs 单次查询超时时间
     * @return 同查询对应的结果集列表
     */
    public static Map<ICube, List<Map<String, Object>>> multiQuery(ThreadPoolExecutor executor, Map<ICube, Query> queryMap,
                                                                   long timeOutMs) {
        checkCubeNotNull(queryMap);
        Map<ICube, Future<List<Map<String, Object>>>> cubeFutureMap = new HashMap<>();
        queryMap.forEach((key, value) -> {
            Future<List<Map<String, Object>>> future = executor.submit(() -> key.query(value));
            cubeFutureMap.put(key, future);
        });
        Map<ICube, List<Map<String, Object>>> res = new HashMap<>(cubeFutureMap.size());
        try {
            for (Map.Entry<ICube, Future<List<Map<String, Object>>>> en : cubeFutureMap.entrySet()) {
                res.put(en.getKey(), en.getValue().get(timeOutMs, TimeUnit.MILLISECONDS));
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
        return multiQueryAndMerge(executor, cube, queryList, TIME_OUT_MS);
    }

    /**
     * 并行执行对一个cube的多个查询并且进行结果合并
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @param timeOutMs 单次查询超时时间
     * @return 同查询顺序对应的结果集列表
     */
    public static List<Map<String, Object>> multiQueryAndMerge(ThreadPoolExecutor executor, final AbstractCube cube,
                                                               List<? extends Query> queryList,
                                                               long timeOutMs) {
        checkCubeNotNull(queryList);
        List<CubeMap<Object>> r = multiRawQueryAndMerge(executor, cube, queryList, timeOutMs);
        List<Map<String, Object>> metric = cube.filterMetric(cube.toMetric(r, queryList.get(0)), queryList.get(0));
        if (!queryList.get(0).getOrders().isEmpty()) {
            cube.order(queryList.get(0).getOrders().toArray(new String[]{}), metric);
        }
        return metric;
    }

    /**
     * 并行执行对一个cube的多个查询并且进行结果合并、同时返回多个查询的结果详细
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @param timeOutMs 单次查询超时时间
     * @return 同查询顺序对应的结果集列表
     */
    public static List<List<Map<String, Object>>> multiQueryAndMergeV1(ThreadPoolExecutor executor, final AbstractCube cube,
                                                                       List<? extends Query> queryList,
                                                                       long timeOutMs) {

        checkCubeNotNull(queryList);
        List<Future<List<CubeMap<Object>>>> futureList = getFutureList(executor, cube, queryList);
        List<List<CubeMap<Object>>> rs = new LinkedList<>();
        try {
            List<List<CubeMap<Object>>> resList = new LinkedList<>();
            for (Future<List<CubeMap<Object>>> f : futureList) {
                resList.add(f.get(timeOutMs, TimeUnit.MILLISECONDS));
            }
            List<CubeMap<Object>> mergeRes = new LinkedList<>();
            for (List<CubeMap<Object>> maps : resList) {
                Set<String> keyList = getQueryDimensionColumns(cube, queryList.get(0));
                mergeRes = MergeUtils.mergeCubeMapList(mergeRes, maps, keyList);
            }
            if (mergeRes.size() == 0) {
                CubeMap<Object> cubeMap = new CubeMap<>();
                mergeRes.add(cubeMap);
            }
            rs.add(mergeRes);
            for (List<CubeMap<Object>> cubeMaps : resList) {
                if (cubeMaps.size() > 0) {
                    rs.add(cubeMaps);
                } else {
                    List<CubeMap<Object>> res = new LinkedList<>();
                    CubeMap<Object> cubeMap = new CubeMap<>();
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
            List<Map<String, Object>> metric = cube.filterMetric(cube.toMetric(r, queryList.get(0)), queryList.get(0));
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
     * @return 同查询顺序对应的结果集列表
     */
    public static List<CubeMap<Object>> multiRawQueryAndMerge(ThreadPoolExecutor executor, final AbstractCube cube,
                                                              List<? extends Query> queryList) {
        return multiRawQueryAndMerge(executor, cube, queryList, TIME_OUT_MS);
    }

    /**
     * 并行执行对一个cube的多个查询并且进行结果合并
     *
     * @param executor  thread pool
     * @param cube      cube
     * @param queryList 查询列表
     * @param timeOutMs 单次查询超时时间
     * @return 同查询顺序对应的结果集列表
     */
    public static List<CubeMap<Object>> multiRawQueryAndMerge(ThreadPoolExecutor executor, final AbstractCube cube,
                                                              List<? extends Query> queryList, long timeOutMs) {
        checkCubeNotNull(queryList);
        List<Future<List<CubeMap<Object>>>> futureList = getFutureList(executor, cube, queryList);
        List<List<CubeMap<Object>>> resList = new LinkedList<>();
        try {
            for (Future<List<CubeMap<Object>>> f : futureList) {
                resList.add(f.get(timeOutMs, TimeUnit.MILLISECONDS));
            }
            List<CubeMap<Object>> res = new LinkedList<>();
            for (List<CubeMap<Object>> cubeMaps : resList) {
                Set<String> keyList = getQueryDimensionColumns(cube, queryList.get(0));
                res = MergeUtils.mergeCubeMapList(res, cubeMaps, keyList);
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
            }
            cols.add(dim.getColumn().getAlias());
        }
        return cols;
    }

    public static List<CubeMap<Object>> multiRawQueryAndMergeV2(ThreadPoolExecutor executor, final AbstractCube cube,
                                                                List<List<Query>> queryCube) {
        return multiRawQueryAndMergeV2(executor, cube, queryCube, TIME_OUT_MS);
    }

    public static List<CubeMap<Object>> multiRawQueryAndMergeV2(ThreadPoolExecutor executor, final AbstractCube cube,
                                                                List<List<Query>> queryCube, long timeOutMs) {
        checkCubeNotNull(queryCube);
        List<List<Future<List<CubeMap<Object>>>>> futureCube = new LinkedList<>();
        for (List<Query> queryList : queryCube) {
            futureCube.add(getFutureList(executor, cube, queryList));
        }
        List<List<CubeMap<Object>>> resList = new LinkedList<>();
        try {
            for (List<Future<List<CubeMap<Object>>>> futureList : futureCube) {
                Set<String> keyList = getQueryDimensionColumns(cube, queryCube.get(0).get(0));
                List<List<CubeMap<Object>>> tmp = new LinkedList<>();
                for (Future<List<CubeMap<Object>>> f : futureList) {
                    tmp.add(f.get(timeOutMs, TimeUnit.MILLISECONDS));
                }
                resList.add(MergeUtils.mergeCubeMapList(keyList, tmp));
            }
            List<CubeMap<Object>> res = new LinkedList<>();
            for (List<CubeMap<Object>> cubeMaps : resList) {
                Set<String> keyList = getQueryDimensionColumns(cube, queryCube.get(0).get(0));
                res = MergeUtils.mergeCubeMapList(res, cubeMaps, keyList);
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

    public static List<List<Map<String, Object>>> multiQueryV2(ThreadPoolExecutor executor, final AbstractCube cube,
                                                               List<List<Query>> queryCube) {
        return multiQueryV2(executor, cube, queryCube, TIME_OUT_MS);
    }

    public static List<List<Map<String, Object>>> multiQueryV2(final ThreadPoolExecutor executor, final AbstractCube cube,
                                                               List<List<Query>> queryCube, final long timeOutMs) {
        checkCubeNotNull(queryCube);
        List<Future<List<Map<String, Object>>>> fList = queryCube.stream()
                .map(queryList -> EXECUTOR_POOL.submit(() -> multiQueryAndMerge(executor, cube, queryList, timeOutMs)))
                .collect(Collectors.toCollection(LinkedList::new));
        return futures(timeOutMs, fList);
    }

    private static List<List<Map<String, Object>>> futures(long timeOutMs, List<Future<List<Map<String, Object>>>> fList) {
        List<List<Map<String, Object>>> res = new LinkedList<>();
        try {
            for (Future<List<Map<String, Object>>> f : fList) {
                res.add(f.get(timeOutMs, TimeUnit.MILLISECONDS));
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

    public static List<List<Map<String, Object>>> multiQueryV3(final ThreadPoolExecutor executor, final AbstractCube cube,
                                                               List<List<Query>> queryCube,
                                                               final long timeOutMs) {
        checkCubeNotNull(queryCube);
        List<Future<List<List<Map<String, Object>>>>> fList = queryCube.stream()
                .map(queryList -> EXECUTOR_POOL.submit(() ->
                        multiQueryAndMergeV1(executor, cube, queryList, timeOutMs)))
                .collect(Collectors.toCollection(LinkedList::new));
        List<List<List<Map<String, Object>>>> res = new LinkedList<>();
        try {
            for (Future<List<List<Map<String, Object>>>> f : fList) {
                res.add(f.get(timeOutMs, TimeUnit.MILLISECONDS));
            }
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            if (e.getCause() instanceof ServiceException) {
                throw (ServiceException) e.getCause();
            }
            throw new RuntimeException(e);
        }
        return res.get(0);
    }

    private static void checkCubeNotNull(Object queryCube) {
        Preconditions.checkNotNull(queryCube, ("query cube can't be null!"));
        if (queryCube instanceof Collection) {
            Preconditions.checkArgument(!CollectionUtils.isEmpty((Collection<?>) queryCube),
                    ("query cube list can't be empty!"));
        } else if (queryCube instanceof Map) {
            Preconditions.checkArgument(!((Map<?, ?>) queryCube).isEmpty(), ("query cube map can't be empty!"));
        }
    }

    private static List<Future<List<CubeMap<Object>>>> getFutureList(ThreadPoolExecutor executor, ICube cube,
                                                                     List<? extends Query> queryList) {
        return queryList.stream()
                .map(query -> executor.submit(() -> cube.rawQuery(query)))
                .collect(Collectors.toCollection(LinkedList::new));
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
