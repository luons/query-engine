package com.luons.engine.core.utils;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

public final class SslUtils {

    public static SSLContext permissiveContext() {
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(new KeyManager[0], new TrustManager[]{new PermissiveTrustManager()}, new SecureRandom());
            return context;
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException("error manipulating SSL", e);
        }
    }

    public static class PermissiveTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }

}
