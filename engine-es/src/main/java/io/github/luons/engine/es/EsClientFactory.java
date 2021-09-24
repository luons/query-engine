package io.github.luons.engine.es;

import com.google.common.base.Preconditions;
import io.github.luons.engine.core.ClientFactory;
import io.github.luons.engine.core.spi.AuthorizationProvider;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.binary.Base64;

import java.net.URI;

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
            Preconditions.checkState((baseUrl != null), ("Base URL is required"));
            maxConnections(1000).maxConnectionsPerRoute(1000).connectTimeout(1000L);
            //EsClientFactory esClientFactory = new EsClientFactory(this, baseUrl);
            EsClientFactory esClientFactory =
                    new EsClientFactory(this, URI.create(baseUrl.toString()));
            esClientFactory.setAuthorization(authorization);
            return esClientFactory;
        }
    }

    public static void main(String[] args) throws Exception {

        EsClient esClient = EsClientFactory.builder()
                .baseUrl("http://localhost:9200/")
                .authorizationProvider(new AuthorizationProvider() {
                    @Override
                    public String getAuthorization() {
                        String encoding = new String(Base64.encodeBase64(("user:password").getBytes()));
                        return "Basic " + encoding;
                    }

                    @Override
                    public boolean refreshToken() {
                        return false;
                    }
                }).build().newEsClientV2();
        esClient.queryDslForAggs("indexName-*", "", "{}");
    }
}
