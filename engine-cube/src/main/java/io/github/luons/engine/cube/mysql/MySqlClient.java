package io.github.luons.engine.cube.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import io.github.luons.engine.core.Client;
import io.github.luons.engine.core.ClientFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class MySqlClient extends Client {

    private String uri;
    private String user;
    private String password;

    protected MySqlClient(ClientFactory factory, String uri) {
        super(factory, uri);
        this.uri = uri;
    }

    protected MySqlClient(ClientFactory factory, String uri, String user, String password) {
        super(factory, uri);
        this.uri = uri;
        this.user = user;
        this.password = password;
    }

    public Long executeQueryCount(String sql) throws Exception {
        Connection connection = MySqlConnect.getConnection(uri, user, password);
        PreparedStatement statement = connection.prepareStatement(sql);
        ResultSet rs = statement.executeQuery();
        if (rs.next()) {
            return rs.getLong((1));
        }
        return 0L;
    }

    public List<Map<String, Object>> executeQuery(String sql) throws Exception {
        Connection connection = MySqlConnect.getConnection(uri, user, password);
        PreparedStatement statement = connection.prepareStatement(sql);
        return getResultData(statement.executeQuery());
    }

    private static List<Map<String, Object>> getResultData(ResultSet rsResultSet) throws SQLException {
        int columnCount = rsResultSet.getMetaData().getColumnCount();
        List<Map<String, Object>> list = new ArrayList<>();
        while (rsResultSet.next()) {
            Map<String, Object> map = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String column = rsResultSet.getMetaData().getColumnName(i);
                if (StringUtils.isBlank(column)) {
                    continue;
                }
                map.put(column.toLowerCase(), rsResultSet.getObject(i));
            }
            list.add(map);
        }
        return list;
    }

    public static class MySqlConnect {

        private static DruidDataSource druidDataSource;

        private static DataSource getDataSource(String uri, String user, String password) throws Exception {
            if (druidDataSource == null) {
                druidDataSource = new DruidDataSource();
                druidDataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
                druidDataSource.setUrl(uri);
                druidDataSource.setUsername(user);
                druidDataSource.setPassword(password);
                druidDataSource.setInitialSize(5);
                druidDataSource.setMinIdle(5);
                druidDataSource.setMaxActive(10);
                // druidDataSource.setMaxIdleTime(600);
                // druidDataSource.setMaxIdleTimeExcessConnections(300);
            }
            return druidDataSource;
        }

        static Connection getConnection(String uri, String user, String password) throws Exception {
            log.debug("Connecting to uri {} user = {} password = {} ...", uri, user, password);
            try {
                DataSource dataSource = getDataSource(uri, user, password);
                assert dataSource != null;
                return dataSource.getConnection();
            } catch (Throwable e) {
                throw new Exception("can not connect mysql Server.");
            }
        }
    }

}
