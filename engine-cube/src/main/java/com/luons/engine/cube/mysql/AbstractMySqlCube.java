package com.luons.engine.cube.mysql;

import com.luons.engine.cube.mapper.CommonMysqlMapper;
import com.ninebot.bigdata.query.common.Pageable;
import com.ninebot.bigdata.query.core.common.AbstractSqlCube;
import com.ninebot.bigdata.query.core.common.Query;
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
        if (pageable == null || pageable.getSize() == null) {
            return "";
        }
        int pIndex = pageable.getIndex() == null ? 1 : pageable.getIndex();
        int pSize = pageable.getSize();
        if (pIndex < 1) {
            pIndex = 1;
        }
        if (pSize > MAX_PAGE_SIZE || pSize < 1) {
            pSize = MAX_PAGE_SIZE;
        }
        return " LIMIT " + ((pIndex - 1) * pSize + " , " + pSize);
    }
}
