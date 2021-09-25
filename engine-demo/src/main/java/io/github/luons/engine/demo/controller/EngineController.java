package io.github.luons.engine.demo.controller;

import io.github.luons.engine.common.CommonException;
import io.github.luons.engine.core.ResponseWrapper;
import io.github.luons.engine.core.cube.AbstractCube;
import io.github.luons.engine.core.spi.Query;
import io.github.luons.engine.cube.CubeRegistry;
import io.github.luons.engine.demo.common.EngineCommon;
import io.github.luons.engine.demo.enums.ResultEnum;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("/engine/query")
public class EngineController {

    @RequestMapping("/{dbName}/{tName}")
    public ResponseWrapper<List<?>> queryMysqlDataV(@PathVariable String dbName, @PathVariable String tName,
                                                    @RequestBody Query query) {
        String cubeKey = dbName + EngineCommon.SPLIT_DVL + tName;
        AbstractCube cube = CubeRegistry.get(cubeKey.toUpperCase());
        if (Objects.isNull(cube)) {
            throw CommonException.asException(ResultEnum.BLANK, "数据源");
        }
        List<Map<String, Object>> list = cube.query(query);
        return ResponseWrapper.success(list);
    }
}
