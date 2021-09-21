package com.luons.engine.core.spi;

public interface AuthorizationProvider {

    String getAuthorization();

    boolean refreshToken();
}
