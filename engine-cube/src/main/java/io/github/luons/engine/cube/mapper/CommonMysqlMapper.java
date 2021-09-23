package io.github.luons.engine.cube.mapper;

import io.github.luons.engine.core.cube.CubeMap;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface CommonMysqlMapper {

    /**
     * 数据写入
     *
     * @param table 表名
     * @param data  Map 数据
     * @return int
     */
    int insert(@Param("table") String table, @Param("data") Map<String, Object> data);

    /**
     * 数据删除
     *
     * @param sql sql语句
     * @return int
     */
    int delete(@Param("sql") String sql, @Param("param") Map<String, Object> data);

    /**
     * 数据条数统计
     *
     * @param sql sql语句
     * @return Long
     */
    Long queryCount(@Param("sql") String sql, @Param("param") Map<String, Object> data);

    /**
     * 数据查询
     *
     * @param sql sql语句
     * @return List
     */
    List<CubeMap<Object>> query(@Param("sql") String sql);

    /**
     * 数据查询
     *
     * @param sql   sql语句
     * @param param param
     * @return List
     */
    List<CubeMap<Object>> query(@Param("sql") String sql, @Param("param") Map<String, Object> param);
}
