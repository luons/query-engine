package io.github.luons.engine.demo;

import io.github.luons.engine.core.CubeRegistry;
import io.github.luons.engine.core.cube.AbstractCube;
import io.github.luons.engine.core.spi.Column;
import io.github.luons.engine.core.spi.Dimension;
import io.github.luons.engine.core.spi.Measure;
import io.github.luons.engine.es.EsCube;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsContext {

    @Bean
    public AbstractCube EsBaseCube() {
        String tableName = "stardb_monitor_system";
        AbstractCube cube =
                new EsCube(tableName)
                        .dimension(new Dimension("ip", new Column("ip", "ip")))
                        .dimension(new Dimension("port", "port"))
                        .dimension(new Dimension("@timestamp", new Column("@timestamp", "timestamp")))
                        .measure(new Measure("system.cpu.total.pct", new Column[]{new Column("system.cpu.total.pct", "system.cpu.total.pct", "avg")}))
                        .measure(new Measure("bytes_received_sum", new Column[]{new Column("mysql.status.global.bytes_received", "mysql_status_global_bytes_received", "sum")}))
                        .measure(new Measure("bytes_received_dir", new Column[]{new Column("mysql_status_global_bytes_received", "mysql.status.global.bytes_received", "derivative")}))
                        .measure(new Measure("bytes_sent_avg", new Column[]{new Column("mysql.status.global.bytes_sent", "bytes_sent", "avg")}))
                        .measure(new Measure("com_delete_max", new Column[]{new Column("mysql.status.global.com_delete", "com_delete", "max")}))
                        .measure(new Measure("com_insert_min", new Column[]{new Column("mysql.status.global.com_insert", "com_insert", "min")}));
        CubeRegistry.registry(tableName.toUpperCase(), cube);
        cube.setCubeName("ES");
        return cube;
    }
}
