package io.github.luons.engine.demo.controller;

import io.github.luons.engine.common.CommonException;
import io.github.luons.engine.core.ResponseWrapper;
import io.github.luons.engine.core.cube.AbstractCube;
import io.github.luons.engine.core.cube.CubeMap;
import io.github.luons.engine.core.spi.Query;
import io.github.luons.engine.cube.CubeRegistry;
import io.github.luons.engine.demo.common.EngineCommon;
import io.github.luons.engine.demo.enums.ResultEnum;
import io.github.luons.engine.demo.service.EsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@RestController
@RequestMapping("/es/query")
public class EsController {

    @Resource
    EsService esService;

    @RequestMapping("/{indexName}/aggs")
    public ResponseWrapper<List<?>> queryEsByCube(@PathVariable String indexName, @RequestBody Query query) {
        String cubeKey = indexName + EngineCommon.SPLIT_DVL + indexName;
        AbstractCube cube = CubeRegistry.get(cubeKey.toUpperCase());
        if (Objects.isNull(cube)) {
            throw CommonException.asException(ResultEnum.BLANK, "数据源");
        }
        List<CubeMap<Object>> list1 = cube.rawQuery(query);
        List<Map<String, Object>> list2 = cube.query(query);
        return ResponseWrapper.success(list2);
    }

    @RequestMapping("/{indexName}/aggs")
    public ResponseWrapper<List<?>> queryEsByAgg(@PathVariable String indexName, @RequestBody String query) {
        if (StringUtils.isBlank(indexName) || StringUtils.isBlank(query)) {
            throw CommonException.asException(ResultEnum.BLANK, "参数");
        }
        if (!indexName.contains("*")) {
            indexName = indexName + "-*";
        }
        try {
            List<Map<String, Object>> list = esService.queryEsAgg(indexName, query);
            return ResponseWrapper.success(list);
        } catch (Exception e) {
            log.error("queryEngineByQuery {} param :{} is error!" + e, indexName, query);
            throw CommonException.asException(ResultEnum.FILTER_OR_SEARCH_ERROR);
        }
    }

    @RequestMapping("/{indexName}")
    public ResponseWrapper<List<?>> queryEngineByQuery(@PathVariable String indexName, @RequestBody String query) {
        if (StringUtils.isBlank(indexName) || StringUtils.isBlank(query)) {
            return ResponseWrapper.error(ResponseWrapper.SC_PARTIAL_CONTENT, ("参数不能为空"));
        }
        if (!indexName.contains("*")) {
            indexName = indexName + "-*";
        }
        try {
            List<Map<String, Object>> list = esService.queryEsScroll(indexName, query);
            return ResponseWrapper.success(list);
        } catch (Exception e) {
            log.error("queryEngineByQuery {} param :{} is error!" + e, indexName, query);
            throw CommonException.asException(ResultEnum.FILTER_OR_SEARCH_ERROR);
        }
    }

}
