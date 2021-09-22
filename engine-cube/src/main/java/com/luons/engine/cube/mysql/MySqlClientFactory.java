package com.luons.engine.cube.mysql;

import com.luons.engine.core.ClientFactory;

public class MySqlClientFactory extends ClientFactory {

    private String user;

    private String password;

    private final String uri;

    public static Builder builder() {
        return new Builder();
    }

    protected MySqlClientFactory(Builder builder, String uri) {
        super(builder);
        this.uri = uri;
    }

    public MySqlClient newSqlClient() {
        return new MySqlClient(this, uri);
    }

    public MySqlClient newSqlClientAuth() {
        return new MySqlClient(this, uri, user, password);
    }

    public static class Builder extends ClientFactory.Builder<Builder> {

        private String user;

        private String password;

        public Builder param(String user, String password) {
            this.user = user;
            this.password = password;
            return this;
        }

        public MySqlClientFactory build() {
            MySqlClientFactory sqlClientFactory = new MySqlClientFactory((this), uri);
            sqlClientFactory.setUser(user);
            sqlClientFactory.setPassword(password);
            return sqlClientFactory;
        }
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
