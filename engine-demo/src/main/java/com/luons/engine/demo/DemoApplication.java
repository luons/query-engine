package com.luons.engine.demo;

import com.ninebot.bigdata.query.core.common.AbstractCube;
import com.ninebot.bigdata.query.core.common.Column;
import com.ninebot.bigdata.query.core.common.Dimension;
import com.ninebot.bigdata.query.core.common.Measure;
import com.ninebot.bigdata.query.cube.CubeRegistry;
import com.ninebot.bigdata.query.cube.mysql.SimpleMySqlCube;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;


@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public AbstractCube mysqlIndexStats() {
        AbstractCube cube =
                new SimpleMySqlCube("innodb_index_stats")
                        .dimension(new Dimension("database_name", "database_name"))
                        .dimension(new Dimension("table_name", "table_name"))
                        .dimension(new Dimension("index_name", "index_name"))
                        .dimension(new Dimension("stat_name", "stat_name"))
                        .measure(new Measure("stat_value", new Column[]{new Column("sum(stat_value)", "statValue", true)}))
                        .measure(new Measure("sample_size", new Column[]{new Column("sum(sample_size)", "sampleSize", true)}));

        CubeRegistry.registry("mysqlIndexStats", cube);
        return cube;
    }

}
