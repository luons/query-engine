package io.github.luons.engine.es;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.luons.engine.core.Client;
import io.github.luons.engine.core.ClientFactory;
import io.github.luons.engine.core.Request;
import io.github.luons.engine.es.EsUtils.EsUtils;
import io.github.luons.engine.utils.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

@Slf4j
@SuppressWarnings("unchecked")
public class EsClient extends Client {

    private String authorization;

    public static final String AUTHORIZATION = "Authorization";

    public static final String SCROLL_ID_KEY = "scrollId";

    protected EsClient(ClientFactory factory, URI baseUrl) {
        super(factory, baseUrl);
    }

    protected EsClient(ClientFactory factory, URI baseUrl, String authorization) {
        super(factory, baseUrl);
        this.authorization = authorization;
    }

    public List<Map<String, Object>> querySql(String sqlQry) throws Exception {
        Request<HttpPost> request = post("/_sql");
        request.header(AUTHORIZATION, authorization);
        Map<String, Object> execute = getEsExecute(request, sqlQry);
        debugLogMessage(request.getRequestUri().toString(), sqlQry, execute.get("took"));
        return getSearchResult(execute);
    }

    public Map<String, Object> querySqlOriginal(String sqlQry) throws Exception {
        Request<HttpPost> request = post("/_sql");
        request.header(AUTHORIZATION, authorization);
        Map<String, Object> execute = getEsExecute(request, sqlQry);
        debugLogMessage(request.getRequestUri().toString(), sqlQry, execute.get("took"));
        return execute;
    }

    private static List<Map<String, Object>> getSearchResult(Map<String, Object> execute) {
        List<Map<String, Object>> fmtRecords = new LinkedList<>();
        Map<String, Object> hitsMap = (Map<String, Object>) execute.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        for (Map<String, Object> hitItem : hitsList) {
            Map<String, Object> source = (Map<String, Object>) hitItem.get("_source");
            // source.put("_id", hitItem.get("_id"));
            source.put("_score", hitItem.get("_score"));
            fmtRecords.add(source);
        }
        Map<String, Object> aggregations = (Map<String, Object>) execute.get("aggregations");
        Map<String, Object> row = new HashMap<>();
        if (aggregations == null) {
            return fmtRecords;
        }
        for (Entry<String, Object> aggrEntry : aggregations.entrySet()) {
            String aggrKey = aggrEntry.getKey();
            Map<String, Object> aggrValue = (Map<String, Object>) aggrEntry.getValue();
            handleAggr(aggrKey, aggrValue, row, fmtRecords);
        }
        // log.info("get hitsList = {} fmtRecords = {} execute {}", hitsList.size(), fmtRecords.size(), JacksonUtils.toJsonString(execute));
        return fmtRecords;
    }

    public List<Map<String, Object>> queryDsl(String indexName, String type, String dslQry, Map<String, String> context) throws Exception {
        Request<HttpPost> request = post(indexName, type, "_search");
        request.header(AUTHORIZATION, authorization);
        request.addParam("scroll", "1m");
        Map<String, Object> execute = getEsExecute(request, dslQry);
        debugLogMessage(request.getRequestUri().toString(), dslQry, execute.get("took"));
        String scrollId = (String) execute.get("_scroll_id");
        context.put(SCROLL_ID_KEY, scrollId);
        List<Map<String, Object>> result = getSearchResult(execute);
        queryDslScroll(scrollId, result);
        return result;
    }

    public List<Map<String, Object>> queryDsl(String indexName, String type, String dslQry) throws Exception {
        if (!dslQry.contains("aggs") || !dslQry.contains("agg")) {
            queryDsl(indexName, type, dslQry, new HashMap<>());
        }
        Request<HttpPost> request = post(indexName, type, "_search");
        request.header(AUTHORIZATION, authorization);
        Map<String, Object> execute = getEsExecute(request, dslQry);
        debugLogMessage(request.getRequestUri().toString(), dslQry, execute.get("took"));
        return getSearchResult(execute);
    }

    public Map<String, Object> queryDslOriginal(String indexName, String type, String dslQry) throws Exception {
        Request<HttpPost> request = post(indexName, type, "_search");
        request.header(AUTHORIZATION, authorization);
        Map<String, Object> execute = getEsExecute(request, dslQry);
        debugLogMessage(request.getRequestUri().toString(), dslQry, execute.get("took"));
        return execute;
    }

    public long countDsl(String indexName, String type, String dslQry) throws Exception {
        Request<HttpPost> request = post(indexName, type, "_count");
        request.header(AUTHORIZATION, authorization);
        Map<String, Object> execute = getEsExecute(request, dslQry);
        debugLogMessage(request.getRequestUri().toString(), dslQry, execute.get("took"));
        return ((Number) execute.get("count")).longValue();
    }

    public List<Map<String, Object>> queryDslForAggs(String indexName, String type, String dslQry) throws Exception {
        Request<HttpPost> request = post(indexName, type, "_search");
        request.header(AUTHORIZATION, authorization);
        // request.addParam("scroll", "1m");
        Map<String, Object> execute = getEsExecute(request, dslQry);
        debugLogMessage(request.getRequestUri().toString(), dslQry, execute.get("took"));
        if (!execute.containsKey("aggregations")) {
            return new ArrayList<>();
        }
        return EsUtils.results((Map<String, Object>) execute.get("aggregations"));
    }

    public List<Map<String, Object>> queryDslScroll(String scrollId, List<Map<String, Object>> scrollList) throws Exception {
        Request<HttpPost> request = post("_search", "scroll");
        request.header(AUTHORIZATION, authorization);
        Map<String, Object> body = new HashMap<>();
        body.put("scroll", "1m");
        body.put("scroll_id", scrollId);

        Map<String, Object> execute = getEsExecute(request, body);
        debugLogMessage(request.getRequestUri().toString(), JacksonUtils.toJson(body), execute.get("took"));
        if (scrollList == null || scrollList.isEmpty()) {
            scrollList = new ArrayList<>();
        }
        List<Map<String, Object>> dataResult = getSearchResult(execute);
        scrollList.addAll(dataResult);
        if (dataResult.isEmpty() || !execute.containsKey("_scroll_id")) {
            return scrollList;
        }
        scrollId = execute.get("_scroll_id").toString();
        if (StringUtils.isNotBlank(scrollId)) {
            queryDslScroll(execute.get("_scroll_id").toString(), scrollList);
        }
        return scrollList;
    }

    public Map<String, Object> getObject(String indexName, String type, String id) throws Exception {
        Request<HttpGet> request = get(indexName, type, id);
        request.header(AUTHORIZATION, this.authorization);

        Map<String, Object> execute = getEsExecute(request, null);
        debugLogMessage(request.getRequestUri().toString(), id, execute.get("took"));
        return getGetResult(execute);
    }

    // TODO
    public Map<String, Object> deleteObject(String indexName, String type, String id) throws Exception {
        Request<HttpDelete> request = delete(indexName, type, id);
        request.header(AUTHORIZATION, this.authorization);
        Map<String, Object> execute = getEsExecute(request, null);
        debugLogMessage(request.getRequestUri().toString(), id, execute.get("took"));
        return getGetResult(execute);
    }

    public void insertObject(String indexName, String type, String id, Map<String, Object> object) throws Exception {
        Request<HttpPut> request;
        if (id == null) {
            request = put(indexName, type);
        } else {
            request = put(indexName, type, id);
        }
        request.header(AUTHORIZATION, this.authorization);
        Map<String, Object> execute = getEsExecute(request, object);
        debugLogMessage(request.getRequestUri().toString(), id, execute.get("took"));
    }

    public void insertObject(String indexName, String type, Map<String, Object> object) throws Exception {
        insertObject(indexName, type, null, object);
    }

    private static void handleAggr(String aggrKey, Map<String, Object> aggrValue, Map<String, Object> row,
                                   List<Map<String, Object>> fmtRecords) {
        if (!aggrValue.containsKey("buckets")) {
            Object value = aggrValue.get("value");
            row.put(aggrKey, value);
            Map<String, Object> newRow = new HashMap<>(row);
            fmtRecords.add(newRow);
            return;
        }
        List<Map<String, Object>> buckets = (List<Map<String, Object>>) aggrValue.get("buckets");
        for (Map<String, Object> bucket : buckets) {
            if (Objects.isNull(bucket) || bucket.isEmpty()) {
                continue;
            }
            for (Entry<String, Object> bucketEntry : bucket.entrySet()) {
                String bucketKey = bucketEntry.getKey();
                if ("doc_count".equals(bucketKey)) {
                    continue;
                }
                if ("key".equals(bucketKey)) {
                    Object value = bucketEntry.getValue();
                    row.put(aggrKey, value);
                } else {
                    Map<String, Object> subAggrValue = (Map<String, Object>) bucketEntry.getValue();
                    handleAggr(bucketKey, subAggrValue, row, fmtRecords);
                }
            }
        }
    }

    private Map<String, Object> getGetResult(Map<String, Object> execute) {
        Object found = execute.get("found");
        if (found instanceof Boolean && (Boolean) found) {
            Object source = execute.get("_source");
            return (Map<String, Object>) source;
        }
        return null;
    }

    private Map<String, Object> getEsExecute(Request<?> request, Object queryObj) throws Exception {
        if (!Objects.isNull(queryObj) && queryObj instanceof Map) {
            request.jsonEntity(queryObj);
        } else if (!Objects.isNull(queryObj) && queryObj instanceof String) {
            StringEntity stringEntity = new StringEntity(queryObj.toString());
            stringEntity.setContentType(ContentType.APPLICATION_JSON.getMimeType());
            request.entity(stringEntity);
        }
        return request.execute(new TypeReference<Map<String, Object>>() {
        });
    }

    private void debugLogMessage(String uri, String query, Object took) {
        if (log.isDebugEnabled()) {
            log.debug("ES query Url: {} Sql: {} result: {}", uri, query, took);
        }
    }
}
