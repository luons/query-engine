package io.github.luons.engine.cube.mysql;

import com.google.common.base.Preconditions;
import io.github.luons.engine.common.Pageable;
import io.github.luons.engine.core.cube.CubeMap;
import io.github.luons.engine.core.spi.Query;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SimpleMySqlCube extends AbstractMySqlCube {

    private final String tableName;

    public SimpleMySqlCube(String tableName) {
        this.tableName = tableName;
        Preconditions.checkNotNull(tableName);
    }

    @Override
    public List<CubeMap<Object>> queryDB(Query query) {
        Map<String, Object> param = new LinkedHashMap<>();
        // select sql
        StringBuilder sqlBuilder = new StringBuilder();
        // count sql
        StringBuilder countBuilder = new StringBuilder();
        sqlBuilder.append(queryToSelectSql(query)).append(" FROM ").append(tableName);
        countBuilder.append("SELECT COUNT(1) FROM ").append(tableName);
        String where = queryToWhereSql(query, param);
        if (StringUtils.isNotBlank(where)) {
            sqlBuilder.append(where);
            countBuilder.append(where);
        }
        String groupBy = queryToGroupBySql(query);
        if (StringUtils.isNotBlank(groupBy)) {
            sqlBuilder.append(groupBy);
            countBuilder.append(groupBy);
        }
        String orderBy = queryToOrderBy(query);
        if (StringUtils.isNotBlank(orderBy)) {
            sqlBuilder.append(orderBy);
        }
        // pageable 1. 查询count 2. 分页查询
        String limits = queryToPageable(query);
        Pageable<?> pageable = query.getPageable();
        if (StringUtils.isNotBlank(limits) && StringUtils.isBlank(groupBy)) {
            sqlBuilder.append(limits);
            Long count = commonMysqlMapper.queryCount(countBuilder.toString(), param);
            pageable.setTotalCount(count);
            int querySize = (pageable.getPageIndex() - 1) * pageable.getPageSize();
            if (count != null && count - querySize < 0) {
                return null;
            }
        }
        String sql = sqlBuilder.toString();
        log.debug("---------SimpleMySqlCube.queryDB sql = {}", sql);
        return commonMysqlMapper.query(sql, param);
    }

}
