package io.github.luons.engine.es;

import com.fasterxml.jackson.core.type.TypeReference;
import com.luons.engine.core.Client;
import com.luons.engine.core.ClientFactory;
import com.luons.engine.core.Request;
import io.github.luons.engine.es.EsUtils.EsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

@Slf4j
@SuppressWarnings("unchecked")
public class EsClient extends Client {

    private String authorization;

    public static final String SCROLL_ID_KEY = "scrollId";

    protected EsClient(ClientFactory factory, URI baseUrl) {
        super(factory, baseUrl);
    }

    protected EsClient(ClientFactory factory, URI baseUrl, String authorization) {
        super(factory, baseUrl);
        this.authorization = authorization;
    }

    public List<Map<String, Object>> querySql(String sqlQry) {
        Request<HttpPost> request = post("/_sql");
        request.header("Authorization", authorization);
        if (log.isDebugEnabled()) {
            log.debug(String.format("ES query Url: %s Sql: %s", request.getRequestUri(), sqlQry));
        }
        try {
            Map execute = getEsRequestExecute(request, sqlQry);
            Object took = execute.get("took");
            log.info("es sql took: {} ", took);
            return getSearchResult(execute);
        } catch (UnsupportedEncodingException e) {
            log.error("Caught unexpected exception: ", e);
            return null;
        }
    }

    public Map<String, Object> querySqlOriginal(String sqlQry) {
        Request<HttpPost> request = post("/_sql");
        request.header("Authorization", authorization);
        if (log.isDebugEnabled()) {
            log.debug(String.format("ES query Url: %s Sql: %s", request.getRequestUri(), sqlQry));
        }
        try {
            Map execute = getEsRequestExecute(request, sqlQry);
            Object took = execute.get("took");
            log.info("es sql took: " + took);
            return execute;
        } catch (UnsupportedEncodingException e) {
            log.error("Caught unexpected exception: ", e);
            return null;
        }
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
        if (aggregations != null) {
            for (Entry<String, Object> aggrEntry : aggregations.entrySet()) {
                String aggrKey = aggrEntry.getKey();
                Map<String, Object> aggrValue = (Map<String, Object>) aggrEntry.getValue();
                handleAggr(aggrKey, aggrValue, row, fmtRecords);
            }
        }
        // log.info("get hitsList = {} fmtRecords = {} execute {}", hitsList.size(), fmtRecords.size(), JacksonUtils.toJsonString(execute));
        return fmtRecords;
    }

    private static void handleAggr(String aggrKey, Map<String, Object> aggrValue,
                                   Map<String, Object> row, List<Map<String, Object>> fmtRecords) {
        if (!aggrValue.containsKey("buckets")) {
            Object value = aggrValue.get("value");
            row.put(aggrKey, value);
            Map<String, Object> newRow = new HashMap<>();
            newRow.putAll(row);
            fmtRecords.add(newRow);
        } else {
            List<Map<String, Object>> buckets = (List<Map<String, Object>>) aggrValue.get("buckets");
            for (Map<String, Object> bucket : buckets) {
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
    }

    public List<Map<String, Object>> queryDsl(String indexName, String type, String dslQry, Map<String, String> context) {
        Request<HttpPost> request = post(indexName, type, "_search");
        request.header("Authorization", authorization);
        request.addParam("scroll", "1m");
        if (log.isDebugEnabled()) {
            log.debug(String.format("ES query Url: %s Dsl: %s", request.getRequestUri(), dslQry));
        }
        try {
            Map execute = getEsRequestExecute(request, dslQry);
            Object took = execute.get("took");
            log.info("es dsl took: " + took);
            String scrollId = (String) execute.get("_scroll_id");
            context.put(SCROLL_ID_KEY, scrollId);
            List<Map<String, Object>> result = getSearchResult(execute);
            queryDslScroll(scrollId, result);
            return result;
        } catch (UnsupportedEncodingException e) {
            log.error("Caught unexpected exception: ", e);
            return null;
        }
    }

    public List<Map<String, Object>> queryDsl(String indexName, String type, String dslQry) {
        if (!dslQry.contains("aggs") || !dslQry.contains("agg")) {
            queryDsl(indexName, type, dslQry, new HashMap<>());
        }
        Request<HttpPost> request = post(indexName, type, "_search");
        request.header("Authorization", authorization);
        if (log.isDebugEnabled()) {
            log.debug(String.format("ES query Url: %s Dsl: %s", request.getRequestUri(), dslQry));
        }
        try {
            Map execute = getEsRequestExecute(request, dslQry);
            Object took = execute.get("took");
            log.info("es dsl {} took {}", dslQry, took);
            return getSearchResult(execute);
        } catch (UnsupportedEncodingException e) {
            log.error("Caught unexpected exception: ", e);
            return null;
        }
    }

    public Map<String, Object> queryDslOriginal(String indexName, String type, String dslQry) {
        Request<HttpPost> request = post(indexName, type, "_search");
        request.header("Authorization", authorization);
        if (log.isDebugEnabled()) {
            log.debug(String.format("ES query Url: %s Dsl: %s", request.getRequestUri(), dslQry));
        }

        try {
            Map execute = getEsRequestExecute(request, dslQry);
            Object took = execute.get("took");
            log.info("es dsl took =  {}", took);
            return execute;
        } catch (UnsupportedEncodingException e) {
            log.error("Caught unexpected exception: ", e);
            return null;
        }
    }

    public long countDsl(String indexName, String type, String dslQry) {
        Request<HttpPost> request = post(indexName, type, "_count");
        request.header("Authorization", authorization);
        if (log.isDebugEnabled()) {
            log.debug(String.format("ES query Url: %s Dsl: %s", request.getRequestUri(), dslQry));
        }
        try {
            Map execute = getEsRequestExecute(request, dslQry);
            Object took = execute.get("took");
            // System.out.println("es dsl took: " + took);
            return getCountResult(execute);
        } catch (UnsupportedEncodingException e) {
            log.error("Caught uOnexpected exception: ", e);
            return 0;
        }
    }

    private long getCountResult(Map<String, Object> execute) {
        return ((Number) execute.get("count")).longValue();
    }

    public List<Map<String, Object>> queryDslForAggs(String indexName, String type, String dslQry) {
        Request<HttpPost> request = post(indexName, type, "_search");
        request.header("Authorization", authorization);
        // request.addParam("scroll", "1m");
        if (log.isDebugEnabled()) {
            log.debug(String.format("ES query Url: %s Dsl: %s", request.getRequestUri(), dslQry));
        }
        try {
            Map execute = getEsRequestExecute(request, dslQry);
            Object took = execute.get("took");
            // System.out.println("es dsl took: " + took);
            if (!execute.containsKey("aggregations")) {
                return new ArrayList<>();
            }
            return EsUtils.results((Map<String, Object>) execute.get("aggregations"));
        } catch (UnsupportedEncodingException e) {
            log.error("Caught unexpected exception: ", e);
            return new ArrayList<>();
        }
    }

    public List<Map<String, Object>> queryDslScroll(String scrollId, List<Map<String, Object>> scrollList) {
        Request<HttpPost> request = post("_search", "scroll");
        request.header("Authorization", authorization);
        Map<String, String> body = new HashMap<>();
        body.put("scroll", "1m");
        body.put("scroll_id", scrollId);

        request.jsonEntity(body);
        Map<String, Object> execute = request.execute(new TypeReference<Map<String, Object>>() {
        });
        Object took = execute.get("took");
        // System.out.println("es scroll took: " + took);
        if (scrollList == null || scrollList.isEmpty()) {
            scrollList = new ArrayList<>();
        }
        List<Map<String, Object>> dataResult = getSearchResult(execute);
        scrollList.addAll(dataResult);
        if (!dataResult.isEmpty() && execute.containsKey("_scroll_id") && StringUtils.isNotBlank(execute.get("_scroll_id").toString())) {
            queryDslScroll(execute.get("_scroll_id").toString(), scrollList);
        }
        return scrollList;
    }

    public void setObject(String indexName, String type, String id, Map<String, Object> object) {
        Request<HttpPut> request;
        if (id == null) {
            request = put(indexName, type);
        } else {
            request = put(indexName, type, id);
        }
        request.header("Authorization", this.authorization);
        request.jsonEntity(object);
        Map execute = request.execute(new TypeReference<Map<String, Object>>() {
        });
        Object took = execute.get("took");
        log.info("es put took {} ", took);
    }

    public void setObject(String indexName, String type, Map<String, Object> object) {
        setObject(indexName, type, null, object);
    }

    public Map<String, Object> getObject(String indexName, String type, String id) {
        Request<HttpGet> request = get(indexName, type, id);
        request.header("Authorization", this.authorization);
        Map execute = request.execute(new TypeReference<Map<String, Object>>() {
        });
        Object took = execute.get("took");
        log.info("es put took {} ", took);
        return getGetResult(execute);
    }

    public Map<String, Object> removeObject(String indexName, String type, String id) {
        Request<HttpDelete> request = delete(indexName, type, id);
        request.header("Authorization", this.authorization);
        Map execute = request.execute(new TypeReference<Map<String, Object>>() {
        });
        Object took = execute.get("took");
        log.info("es delete took {} ", took);
        return getGetResult(execute);
    }

    private Map<String, Object> getGetResult(Map<String, Object> execute) {
        Object found = execute.get("found");
        if (found instanceof Boolean && (Boolean) found) {
            Object source = execute.get("_source");
            return (Map<String, Object>) source;
        }
        return null;
    }

    private Map getEsRequestExecute(Request<HttpPost> request, String dslQry) throws UnsupportedEncodingException {
        StringEntity stringEntity = new StringEntity(dslQry);
        stringEntity.setContentType(ContentType.APPLICATION_JSON.getMimeType());
        request.entity(stringEntity);
        return request.execute(new TypeReference<Map<String, Object>>() {
        });
    }
}
