package io.github.luons.engine.demo.controller;

import io.github.luons.engine.core.ResponseWrapper;
import io.github.luons.engine.core.cube.AbstractCube;
import io.github.luons.engine.core.spi.Query;
import io.github.luons.engine.cube.CubeRegistry;
import io.github.luons.engine.utils.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/test")
public class TestController {
    
    @RequestMapping("/engine/query")
    public ResponseWrapper<List<Map<String, Object>>> queryMysqlIndexStats(@RequestBody Query query) {
        ResponseWrapper<List<Map<String, Object>>> messageWrapper = new ResponseWrapper<>();
        log.debug(JacksonUtils.toJson(query));
        AbstractCube cube = CubeRegistry.get("mysqlIndexStats");
        List<Map<String, Object>> list = cube.query(query);
        log.debug(JacksonUtils.toJson(list));

        messageWrapper.setData(list);
        return messageWrapper;
    }


}
