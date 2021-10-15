package io.github.luons.engine.demo;

import io.github.luons.engine.core.cube.AbstractCube;
import io.github.luons.engine.core.spi.Column;
import io.github.luons.engine.core.spi.Dimension;
import io.github.luons.engine.core.spi.Measure;
import io.github.luons.engine.cube.CubeRegistry;
import io.github.luons.engine.es.EsCube;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsContext {

    @Bean
    public AbstractCube EsBaseCube() {
        String tableName = "stardb_monitor_mysql";
        AbstractCube cube =
                new EsCube(tableName)
                        .dimension(new Dimension("ip", new Column("ip", "ip")))
                        .dimension(new Dimension("port", "port"))
                        .dimension(new Dimension("@timestamp", new Column("@timestamp", "timestamp")))
                        .measure(new Measure("bytes_received_sum", new Column[]{new Column("mysql.status.global.bytes_received", "bytes_received_sum", "sum")}))
                        .measure(new Measure("innodb_data_read_sum", new Column[]{new Column("mysql.status.global.innodb_data_read", "innodb_data_read_sum", "sum")}))
                        .measure(new Measure("innodb_data_reads_avg", new Column[]{new Column("mysql.status.global.innodb_data_reads", "clients_sum", "avg")}))
                        .measure(new Measure("innodb_data_writes_sum", new Column[]{new Column("mysql.status.global.innodb_data_writes", "clients_sum", "sum")}))
                        .measure(new Measure("innodb_data_written_avg", new Column[]{new Column("mysql.status.global.innodb_data_written", "connects_avg", "avg")}));
        CubeRegistry.registry(tableName.toUpperCase(), cube);
        cube.setCubeName("ES");
        return cube;
    }
}
