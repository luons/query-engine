package io.github.luons.engine.demo.config;

import io.github.luons.engine.core.spi.AuthorizationProvider;
import io.github.luons.engine.es.EsClient;
import io.github.luons.engine.es.EsClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class EsClientConfig {

    @Value("${spring.es.url}")
    private String esUrl;

    @Value("${spring.es.user}")
    private String esUser;

    @Value("${spring.es.password}")
    private String esPwd;

    @Bean
    public EsClient esClient() {
        return EsClientFactory.builder()
                .baseUrl(esUrl)
                .authorizationProvider(new AuthorizationProvider() {
                    @Override
                    public String getAuthorization() {
                        String auth = new String(Base64.encodeBase64((esUser + ":" + esPwd).getBytes()));
                        return "Basic " + auth;
                    }

                    @Override
                    public boolean refreshToken() {
                        return false;
                    }
                }).build().newEsClientV2();
    }
}

