package io.github.luons.engine.cube.mysql;

import io.github.luons.engine.common.Pageable;
import io.github.luons.engine.core.cube.AbstractSqlCube;
import io.github.luons.engine.core.spi.Query;
import io.github.luons.engine.cube.mapper.CommonMysqlMapper;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class AbstractMySqlCube extends AbstractSqlCube {

    @Autowired
    protected CommonMysqlMapper commonMysqlMapper;

    private static final int MAX_PAGE_SIZE = 100;

    protected String queryToPageable(Query query) {
        if (query == null) {
            return "";
        }
        Pageable pageable = query.getPageable();
        if (pageable == null || pageable.getTotalCount() == null) {
            return "";
        }
        int pIndex = pageable.getPageIndex() == null ? 1 : pageable.getPageIndex();
        int pSize = pageable.getPageSize();
        if (pIndex < 1) {
            pIndex = 1;
        }
        if (pSize > MAX_PAGE_SIZE || pSize < 1) {
            pSize = MAX_PAGE_SIZE;
        }
        return " LIMIT " + ((pIndex - 1) * pSize + " , " + pSize);
    }
}
