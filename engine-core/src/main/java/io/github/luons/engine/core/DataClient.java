package io.github.luons.engine.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import io.github.luons.engine.core.utils.DateUtils;
import io.github.luons.engine.utils.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;

import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

@Slf4j
@SuppressWarnings("unchecked")
public class DataClient extends Client {

    private final Joiner joiner = Joiner.on(",");

    private static final String DIM_CODE = "dim_code";

    private static final String PRODUCT_ID = "product_id";

    protected DataClient(ClientFactory factory, URI baseUrl) {
        super(factory, baseUrl);
    }

    public List<Map<String, Object>> getProducts() {
        Request<HttpGet> request = get("/products");
        return getExecuteDataByRequest(request);
    }

    public List<Map<String, Object>> getDims(int productId) {
        Request<HttpGet> request = get("/dims");
        request.addParam(PRODUCT_ID, productId);
        return getExecuteDataByRequest(request);
    }

    public List<Map<String, Object>> getDimItems(int productId, String dimCode) {
        Request<HttpGet> request = get("/dim_items");
        request.addParam(PRODUCT_ID, productId);
        if (StringUtils.isNotBlank(dimCode)) {
            request.addParam(DIM_CODE, dimCode);
        }
        return getExecuteDataByRequest(request);
    }

    public List<Map<String, Object>> getMetrics(int productId, List<String> dimCodes, List<Integer> metricIds) {
        Request<HttpGet> request = get("/metrics");
        request.addParam(PRODUCT_ID, productId);
        if (dimCodes != null && dimCodes.size() == 0) {
            request.addParam(("dim_codes"), joiner.join(dimCodes));
        }
        if (metricIds != null) {
            request.addParam(("metric_ids"), joiner.join(metricIds));
        }
        return getExecuteDataByRequest(request);
    }

    public List<Map<String, Object>> getData(int productId, List<String> dims, List<Integer> metricIds, String date) {
        return getData(productId, dims, metricIds, date, null);
    }

    public List<Map<String, Object>> getData(int productId, List<String> dims, List<Integer> metricIds, String date,
                                             List<Map<String, Object>> filters) {
        Map<String, Object> param = new HashMap<>();
        param.put("product_id", productId);
        param.put("metrics", metricIds);
        param.put("dims", dims);
        param.put("begin_date", date);
        param.put("end_date", date);
        param.put("period", "day");
        if (filters != null && filters.size() > 0) {
            param.put("dim_filters", filters);
        }
        Request<HttpPost> request = post(("/data")).jsonEntity(Collections.singletonList(param));
        return getExecuteDataByRequest(request);
    }

    public Map<String, Long> getDataReadyTime(int productId, List<String> dims, List<Integer> metricIds, String date) throws Exception {
        Request<HttpGet> request = get("/ready");
        request.addParam(PRODUCT_ID, productId);
        request.addParam(("date"), date);
        if (dims != null) {
            request.addParam(("dim_codes"), joiner.join(dims));
        }
        if (metricIds != null) {
            request.addParam(("metric_ids"), joiner.join(metricIds));
        }
        Map<String, Object> execute = getExecuteByRequest(request);
        log.debug(JacksonUtils.toJsonString(execute));
        int status = (Integer) execute.get("status");
        if (1 == status) {
            return new HashMap<>();
        }
        Map<String, String> readyTime;
        Map<String, Long> result = new HashMap<>();
        try {
            Map<String, Object> data = (Map<String, Object>) execute.get("data");
            readyTime = (Map<String, String>) data.get("ready_time");
        } catch (Exception e) {
            readyTime = new HashMap<>();
            log.error("execute get data parse is exception " + e);
        }
        for (Entry<String, String> entry : readyTime.entrySet()) {
            if (StringUtils.isBlank(entry.getValue())) {
                continue;
            }
            result.put(entry.getKey(), DateUtils.toDate(entry.getValue()).getTime());
        }
        return result;
    }

    public List<Map<String, Object>> getData(int productId, List<String> dims, Integer metricId, String date) {
        return getData(productId, dims, Collections.singletonList(metricId), date);
    }

    public List<Map<String, Object>> getData(int productId, List<String> dims, Integer metricId, String date, List<Map<String, Object>> filters) {
        return getData(productId, dims, Collections.singletonList(metricId), date, filters);
    }

    public Map<String, Object> getData(int productId, List<String> dims, List<Integer> metricIds,
                                       List<Map<String, Object>> filters, String beginDate, String endDate, boolean aggregate,
                                       String period, int pageSize, int pageNum, List<Map<String, Object>> sorters,
                                       Map<String, String> extFormula) {
        Map<String, Object> param = new HashMap<>();
        param.put("product_id", productId);
        param.put("metrics", metricIds);
        if (extFormula != null && extFormula.size() > 0) {
            param.put("ext_formula", extFormula);
        }
        param.put("dims", dims);
        param.put("begin_date", beginDate);
        param.put("end_date", endDate);
        if (period == null) {
            param.put("period", "day");
        } else {
            param.put("period", period);
        }
        if (filters != null && filters.size() > 0) {
            param.put("dim_filters", filters);
        }
        param.put("aggregate", aggregate);
        if (pageSize > 0) {
            param.put("page_size", pageSize);
        }
        if (pageNum > 0) {
            param.put("page_num", pageNum);
        }
        if (sorters != null) {
            param.put("sorters", sorters);
        }
        Request<HttpPost> request = post("/data").jsonEntity(Collections.singletonList(param));
        List<Map<String, Object>> res = getExecuteDataByRequest(request);
        if (res.size() > 0) {
            return res.get(0);
        }
        return null;
    }

    private List<Map<String, Object>> getExecuteDataByRequest(Request<?> request) {
        Map<String, Object> execute = getExecuteByRequest(request);
        if (execute == null || execute.size() == 0 || execute.get("data") == null) {
            return new ArrayList<>();
        }
        Object dataObj = execute.get("data");
        if (dataObj instanceof List) {
            return (List<Map<String, Object>>) dataObj;
        } else if (dataObj instanceof Map) {
            return new ArrayList<>((Collection<? extends Map<String, Object>>) dataObj);
        }
        return new ArrayList<>();
    }

    private Map<String, Object> getExecuteByRequest(Request<?> request) {
        if (Objects.isNull(request)) {
            return new HashMap<>();
        }
        return request.execute(new TypeReference<Map<String, Object>>() {
        });
    }
}
