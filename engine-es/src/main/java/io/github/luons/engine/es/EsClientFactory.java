package io.github.luons.engine.es;

import com.google.common.base.Preconditions;
import io.github.luons.engine.core.ClientFactory;
import io.github.luons.engine.core.spi.AuthorizationProvider;
import io.github.luons.engine.utils.JacksonUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.binary.Base64;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class EsClientFactory extends ClientFactory {

    private String authorization;

    private final URI baseUrl;

    public EsClientFactory(Builder builder, URI baseUrl) {
        super(builder);
        this.baseUrl = baseUrl;
    }

    public static Builder builder() {
        return new Builder();
    }

    public EsClient newEsClient() {
        return new EsClient(this, baseUrl);
    }

    public EsClient newEsClientV2() {
        return new EsClient(this, baseUrl, authorization);
    }

    public static class Builder extends ClientFactory.Builder<Builder> {

        private String authorization;

        public Builder authorization(String authorization) {
            this.authorization = authorization;
            return this;
        }

        public EsClientFactory build() {
            Preconditions.checkState(baseUrl != null, "Base URL is required");

            maxConnections(1000).maxConnectionsPerRoute(1000).connectTimeout(1000L);
            //EsClientFactory esClientFactory = new EsClientFactory(this, baseUrl);
            EsClientFactory esClientFactory =
                    new EsClientFactory(this, URI.create(String.format(baseUrl.toString())));
            esClientFactory.setAuthorization(authorization);
            return esClientFactory;
        }
    }

    public static void main(String[] args) throws Exception {

        EsClient esClient = EsClientFactory.builder()
                .baseUrl("http://10.64.2.95:9200/")
                .authorizationProvider(new AuthorizationProvider() {
                    @Override
                    public String getAuthorization() {
                        String encoding = new String(Base64.encodeBase64(("curl_user:curlpwd").getBytes()));
                        return "Basic " + encoding;
                    }

                    @Override
                    public boolean refreshToken() {
                        return false;
                    }
                }).build().newEsClientV2();

        Map<String, String> map = new HashMap<>();
        List<Map<String, Object>> mapAggs2 = esClient.queryDslForAggs("app_client_api_log-*", "", "{\"aggs\":{\"time\":{\"date_histogram\":{\"field\":\"@timestamp\",\"calendar_interval\":\"1d\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1},\"aggs\":{\"time_cost\":{\"range\":{\"field\":\"time_cost\",\"ranges\":[{\"from\":0,\"to\":1000},{\"from\":1000,\"to\":100000}],\"keyed\":true}}}}},\"size\":0,\"docvalue_fields\":[{\"field\":\"@timestamp\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"host\":\"api5-bj.ninebot.cn\"}},{\"match_phrase\":{\"host\":\"api5-bj.ninebot.com\"}}],\"minimum_should_match\":1}},{\"range\":{\"@timestamp\":{\"format\":\"epoch_millis\",\"gte\":1603036800000,\"lt\":1603123200000}}}]}}}");

        System.out.println(JacksonUtils.toJsonString(mapAggs2));
        List<Map<String, Object>> mapAv = esClient.queryDsl("s2_burying_point_log-*", "", "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"$distinct_id\":\"EVT7-10\"}},{\"range\":{\"$time\":{\"gte\":1591774057000}}},{\"range\":{\"$time\":{\"lt\":1591774242000}}},{\"exists\":{\"field\":\"properties.error_code\"}},{\"term\":{\"properties.cloud_ssl\":\"ssl://120.92.209.166:8883\"}}]}},\"_source\":{\"includes\":[\"$distinct_id\",\"$time\",\"properties.error_code\",\"properties.error_info\",\"$event\"]}}", map);
        List<Map<String, Object>> mapAggs1 = esClient.queryDslForAggs("app_steeldust_mysql_binlog-*", "", "{\"aggs\":{\"data_name\":{\"range\":{\"field\":\"bat1_temp1\",\"ranges\":[{\"from\":0,\"to\":10},{\"from\":10,\"to\":20},{\"from\":20,\"to\":30},{\"from\":30,\"to\":40},{\"from\":40,\"to\":50},{\"from\":50,\"to\":60},{\"from\":60,\"to\":70},{\"from\":70}],\"keyed\":true}}},\"size\":0,\"docvalue_fields\":[{\"field\":\"@timestamp\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"match_phrase\":{\"TABLE_NAME\":{\"query\":\"vehicle_info\"}}},{\"range\":{\"@timestamp\":{\"gte\":1589198765356,\"lte\":1589285165356,\"format\":\"epoch_millis\"}}},{\"match_phrase\":{\"TABLE_NAME\":{\"query\":\"vehicle_info\"}}}]}}}");
        List<Map<String, Object>> maps = esClient.queryDsl("s2_burying_point_log-*", "", "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"$distinct_id\":\"EVT6-2-10\"}},{\"range\":{\"$time\":{\"gte\":1588852530000}}},{\"range\":{\"$time\":{\"lt\":1588852560000}}}]}}}");
        List<Map<String, Object>> mapAggs = esClient.queryDslForAggs("passport_interface_monitor_log-*", "", "{\"aggs\":{\"time\":{\"date_histogram\":{\"field\":\"@timestamp\",\"interval\":\"30s\",\"time_zone\":\"Asia/Shanghai\",\"min_doc_count\":1},\"aggs\":{\"env_type\":{\"terms\":{\"field\":\"env_type\"},\"aggs\":{\"uri\":{\"terms\":{\"field\":\"uri\",\"size\":7,\"order\":{\"runtime\":\"desc\"}},\"aggs\":{\"runtime\":{\"avg\":{\"field\":\"runtime\"}}}}}}}}},\"size\":0,\"_source\":{\"excludes\":[]},\"docvalue_fields\":[{\"field\":\"@timestamp\",\"format\":\"date_time\"}],\"query\":{\"bool\":{\"must\":[{\"bool\":{\"minimum_should_match\":1,\"should\":[{\"match_phrase\":{\"uri\":\"/v3/region\"}},{\"match_phrase\":{\"uri\":\"/v3/user/login\"}},{\"match_phrase\":{\"uri\":\"/v3/user\"}},{\"match_phrase\":{\"uri\":\"/v1/user/subinfo\"}},{\"match_phrase\":{\"uri\":\"/v3/code/phone\"}},{\"match_phrase\":{\"uri\":\"/v3/user/login/apple\"}},{\"match_phrase\":{\"uri\":\"/v3/user/bind/weixin\"}}]}},{\"range\":{\"@timestamp\":{\"gte\":1587707400000,\"lt\":1587707700000,\"format\":\"epoch_millis\"}}}]}}}");
        if (maps == null || maps.size() == 0) {
            return;
        }

        System.out.println(JacksonUtils.toJsonString(maps));

        Map<String, Object> stringObjectMap = maps.get(0);
        System.out.println(stringObjectMap.get("title"));
        System.out.println(stringObjectMap.get("price"));

        String _id = (String) stringObjectMap.get("_id");
        System.out.println(_id);

        stringObjectMap.remove("_id");
        stringObjectMap.remove("_score");
        stringObjectMap.put("price", 90);
        esClient.setObject("test_index", "test_type", _id, stringObjectMap);

        Map<String, Object> object = esClient.getObject("test_index", "test_type", _id);
        System.out.println(JacksonUtils.toJsonString(object));

    }
}
