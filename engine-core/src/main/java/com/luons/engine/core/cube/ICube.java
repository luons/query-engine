package com.luons.engine.core.cube;

import com.luons.engine.core.spi.Query;

import java.util.List;
import java.util.Map;

public interface ICube {

    /**
     * 查询cube,返回结果为未处理后的维度指标计算和排序列表
     *
     * @param query 查询对象
     * @return List
     */
    List<CubeMap<Object>> rawQuery(Query query);

    /**
     * 查询cube,返回结果为处理后的维度指标计算和排序列表
     *
     * @param query 查询对象
     * @return List
     */
    List<Map<String, Object>> query(Query query);
}
