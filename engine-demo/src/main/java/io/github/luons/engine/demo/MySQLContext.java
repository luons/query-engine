package io.github.luons.engine.demo;

import io.github.luons.engine.core.cube.AbstractCube;
import io.github.luons.engine.core.spi.Column;
import io.github.luons.engine.core.spi.Dimension;
import io.github.luons.engine.core.spi.Measure;
import io.github.luons.engine.core.CubeRegistry;
import io.github.luons.engine.cube.mysql.SimpleMySqlCube;
import io.github.luons.engine.demo.common.EngineCommon;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MySQLContext {

    private static final String DB_MYSQL = "mysql";

    @Bean
    public AbstractCube mysqlIndexStats() {
        String tableName = "innodb_index_stats";
        AbstractCube cube =
                new SimpleMySqlCube(tableName)
                        .dimension(new Dimension("database_name", "database_name"))
                        .dimension(new Dimension("table_name", "table_name"))
                        .dimension(new Dimension("index_name", "index_name"))
                        .dimension(new Dimension("stat_name", "stat_name"))
                        .measure(new Measure("stat_value", new Column[]{new Column("sum(stat_value)", "statValue", true)}))
                        .measure(new Measure("sample_size", new Column[]{new Column("sum(sample_size)", "sampleSize", true)}));

        CubeRegistry.registry((DB_MYSQL + EngineCommon.SPLIT_DVL + tableName).toUpperCase(), cube);
        return cube;
    }
}
