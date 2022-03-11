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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@SuppressWarnings("unchecked")
public class EsClient extends Client {

    private HashMap<String, Object> headers;

    public static final String AUTHORIZATION = "Authorization";

    public static final String SCROLL_ID_KEY = "scrollId";

    protected static final int QUERY_COUNT = 2000;

    protected EsClient(ClientFactory factory, URI baseUrl) {
        super(factory, baseUrl);
    }

    protected EsClient(ClientFactory factory, URI baseUrl, HashMap<String, Object> headers) {
        super(factory, baseUrl);
        this.headers = headers;
    }

    public List<Map<String, Object>> querySql(String sqlQry) throws Exception {
        Request<HttpPost> request = post("/_sql");
        if (headers != null) {
            for (String key : headers.keySet()) {
                request.header(key, headers.get(key));
            }
        }
        Map<String, Object> execute = getEsExecute(request, sqlQry);
        debugLogMessage(request.getRequestUri().toString(), sqlQry, execute);
        return getSearchResult(execute, true);
    }

    private static List<Map<String, Object>> getSearchResult(Map<String, Object> execute) {
        return getSearchResult(execute, false);
    }

    private static List<Map<String, Object>> getSearchResult(Map<String, Object> execute, boolean isSql) {
        List<Map<String, Object>> fmtRecords = new LinkedList<>();
        if (isSql) {
            if (!execute.containsKey("columns") || !execute.containsKey("rows")) {
                return fmtRecords;
            }
            List<String> columns = ((List<Map<String, Object>>) execute.get("columns")).stream()
                    .filter(data -> data.containsKey("name")).map(data -> (String) data.get("name"))
                    .collect(Collectors.toList());
            return ((List<List<Object>>) execute.get("rows")).stream()
                    .filter(rows -> !Objects.isNull(rows) && rows.size() == columns.size())
                    .map(rows -> IntStream.range(0, columns.size()).boxed()
                            .collect(Collectors.toMap(columns::get, rows::get, (a, b) -> b)))
                    .collect(Collectors.toCollection(LinkedList::new));
        }
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

    public List<Map<String, Object>> queryDslDefault(String indexName, String type, String dslQry) throws Exception {
        Request<HttpPost> request = post(indexName, type, "_search");
        if (headers != null) {
            for (String key : headers.keySet()) {
                request.header(key, headers.get(key));
            }
        }
        Map<String, Object> execute = getEsExecute(request, dslQry);
        debugLogMessage(request.getRequestUri().toString(), dslQry, execute.get("took"));
        return getSearchResult(execute);
    }

    public List<Map<String, Object>> queryDsl(String indexName, String type, String dslQry, Map<String, String> context) throws Exception {
        Request<HttpPost> request = post(indexName, type, "_search");
        if (headers != null) {
            for (String key : headers.keySet()) {
                request.header(key, headers.get(key));
            }
        }
        request.addParam("scroll", "1m");
        Map<String, Object> execute = getEsExecute(request, dslQry);
        debugLogMessage(request.getRequestUri().toString(), dslQry, execute.get("took"));
        String scrollId = (String) execute.get("_scroll_id");
        context.put(SCROLL_ID_KEY, scrollId);
        List<Map<String, Object>> result = getSearchResult(execute);
        // 判断是否需要scroll
        if (result.size() >= (QUERY_COUNT / 2)) {
            queryDslScroll(scrollId, result);
        }
        clearScroll();
        return result;
    }

    public List<Map<String, Object>> queryDsl(String indexName, String type, String dslQry) throws Exception {
        if (!dslQry.contains("aggs") || !dslQry.contains("agg")) {
            queryDsl(indexName, type, dslQry, new HashMap<>());
        }
        Request<HttpPost> request = post(indexName, type, "_search");
        if (headers != null) {
            for (String key : headers.keySet()) {
                request.header(key, headers.get(key));
            }
        }
        Map<String, Object> execute = getEsExecute(request, dslQry);
        debugLogMessage(request.getRequestUri().toString(), dslQry, execute.get("took"));
        return getSearchResult(execute);
    }

    public long countDsl(String indexName, String type, String dslQry) throws Exception {
        Request<HttpPost> request = post(indexName, type, "_count");
        if (headers != null) {
            for (String key : headers.keySet()) {
                request.header(key, headers.get(key));
            }
        }
        Map<String, Object> execute = getEsExecute(request, dslQry);
        debugLogMessage(request.getRequestUri().toString(), dslQry, execute.get("took"));
        return ((Number) execute.get("count")).longValue();
    }

    public List<Map<String, Object>> queryDslForAggs(String indexName, String type, String dslQry) throws Exception {
        Request<HttpPost> request = post(indexName, type, "_search");
        if (headers != null) {
            for (String key : headers.keySet()) {
                request.header(key, headers.get(key));
            }
        }
        Map<String, Object> execute = getEsExecute(request, dslQry);
        debugLogMessage(request.getRequestUri().toString(), dslQry, execute.get("took"));
        if (!execute.containsKey("aggregations")) {
            return new ArrayList<>();
        }
        return EsUtils.results((Map<String, Object>) execute.get("aggregations"));
    }

    private List<Map<String, Object>> queryDslScroll(String scrollId, List<Map<String, Object>> scrollList) throws Exception {
        if (StringUtils.isBlank(scrollId)) {
            return new ArrayList<>();
        }
        Request<HttpPost> request = post("_search", "scroll");
        if (headers != null) {
            for (String key : headers.keySet()) {
                request.header(key, headers.get(key));
            }
        }
        Map<String, Object> body = new HashMap<>();
        body.put("scroll", "1m");
        body.put("scroll_id", scrollId);
        Map<String, Object> execute = getEsExecute(request, body);
        debugLogMessage(request.getRequestUri().toString(), JacksonUtils.toJson(body), execute.get("took"));
        if (scrollList == null || scrollList.isEmpty()) {
            scrollList = new ArrayList<>();
        }
        List<Map<String, Object>> dataResult = getSearchResult(execute);
        if (dataResult.isEmpty() || !execute.containsKey("_scroll_id")) {
            return scrollList;
        }
        scrollList.addAll(dataResult);
        scrollId = execute.get("_scroll_id").toString();
        if (StringUtils.isNotBlank(scrollId)) {
            queryDslScroll(execute.get("_scroll_id").toString(), scrollList);
        }
        return scrollList;
    }

    public void insertObject(String indexName, String type, String id, Map<String, Object> object) throws Exception {
        Request<HttpPut> request;
        if (id == null) {
            request = put(indexName, type);
        } else {
            request = put(indexName, type, id);
        }
        if (this.headers != null) {
            for (String key : this.headers.keySet()) {
                request.header(key, this.headers.get(key));
            }
        }
        Map<String, Object> execute = getEsExecute(request, object);
        debugLogMessage(request.getRequestUri().toString(), id, execute.get("took"));
    }

    public void insertObject(String indexName, String type, Map<String, Object> object) throws Exception {
        insertObject(indexName, type, null, object);
    }

    public Map<String, Object> getObject(String indexName, String type, String id) throws Exception {
        Request<HttpGet> request = get(indexName, type, id);
        if (this.headers != null) {
            for (String key : this.headers.keySet()) {
                request.header(key, this.headers.get(key));
            }
        }

        Map<String, Object> execute = getEsExecute(request, null);
        debugLogMessage(request.getRequestUri().toString(), id, execute.get("took"));
        return getGetResult(execute);
    }

    public Map<String, Object> deleteObject(String indexName, String type, String id) throws Exception {
        Request<HttpDelete> request = delete(indexName, type, id);
        if (this.headers != null) {
            for (String key : this.headers.keySet()) {
                request.header(key, this.headers.get(key));
            }
        }
        Map<String, Object> execute = getEsExecute(request, null);
        debugLogMessage(request.getRequestUri().toString(), id, execute.get("took"));
        return getGetResult(execute);
    }

    public void clearScroll() throws Exception {
        Request<HttpDelete> request = delete("/_search/scroll/_all");
        if (this.headers != null) {
            for (String key : this.headers.keySet()) {
                request.header(key, this.headers.get(key));
            }
        }
        Map<String, Object> execute = getEsExecute(request, null);
        getGetResult(execute);
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
