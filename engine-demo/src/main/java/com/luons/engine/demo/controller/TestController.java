package com.luons.engine.demo.controller;

import com.ninebot.bigdata.query.core.common.AbstractCube;
import com.ninebot.bigdata.query.core.common.Query;
import com.ninebot.bigdata.query.core.http.ResponseWrapper;
import com.ninebot.bigdata.query.cube.CubeRegistry;
import com.ninebot.bigdata.query.utils.JacksonUtils;
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

    /**
     */
    @RequestMapping("/engine/query")
    public ResponseWrapper<List<Map<String, Object>>> queryMysqlIndexStats(@RequestBody Query query) {
        ResponseWrapper<List<Map<String, Object>>> messageWrapper = new ResponseWrapper<>();
        log.debug(JacksonUtils.toJsonString(query));
        AbstractCube cube = CubeRegistry.get("mysqlIndexStats");
        List<Map<String, Object>> list = cube.query(query);
        log.debug(JacksonUtils.toJsonString(list));

        messageWrapper.setData(list);
        return messageWrapper;
    }


}
