package io.github.luons.engine.demo.service;

import io.github.luons.engine.es.EsClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class EsService {

    @Autowired
    private EsClient esClient;

    public List<Map<String, Object>> queryEsAgg(String index, String queryJson) throws Exception {
        return queryEsAgg(index, (""), queryJson);
    }

    public List<Map<String, Object>> queryEsAgg(String index, String type, String queryJson) throws Exception {
        return esClient.queryDslForAggs(index, type, queryJson);
    }

    public List<Map<String, Object>> queryEsScroll(String index, String queryDsl) throws Exception {
        return queryEsScroll(index, (""), queryDsl);
    }

    public List<Map<String, Object>> queryEsScroll(String index, String type, String queryDsl) throws Exception {
        return esClient.queryDsl(index, type, queryDsl, new HashMap<>());
    }
}
