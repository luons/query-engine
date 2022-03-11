package io.github.luons.engine.demo.config;

import io.github.luons.engine.core.spi.AuthorizationProvider;
import io.github.luons.engine.es.EsClient;
import io.github.luons.engine.es.EsClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@Slf4j
@Configuration
public class EsClientConfig {
    static final HashMap<String, Object> headers = new HashMap<>();
    @Value("${spring.es.url:http://localhost:9200}")
    private String esUrl;

    @Value("${spring.es.user:user}")
    private String esUser;

    @Value("${spring.es.password:pwd}")
    private String esPwd;

    @Bean
    public EsClient esClient() {
        String auth = "Basic " + new String(Base64.encodeBase64((esUser + ":" + esPwd).getBytes()));
        headers.put("Authorization", auth);
        return EsClientFactory.builder()
                .baseUrl(esUrl)
                .authorizationProvider(new AuthorizationProvider() {
                    @Override
                    public String getAuthorization() {
                        return auth;
                    }
                    @Override
                    public boolean refreshToken() {
                        return false;
                    }
                })
                .headers(headers)
                .build().newEsClientV2();
    }
}

