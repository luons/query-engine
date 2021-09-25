package io.github.luons.engine.demo;

import io.github.luons.engine.core.cube.AbstractCube;
import io.github.luons.engine.core.spi.Dimension;
import io.github.luons.engine.cube.CubeRegistry;
import io.github.luons.engine.demo.common.EngineCommon;
import io.github.luons.engine.es.EsCube;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EsContext {

    private static final String ES_CN = "es_s2";

    @Bean
    public AbstractCube EsBaseCube() {
        String tableName = "s2_burying_point_log";
        AbstractCube cube =
                new EsCube(tableName)
                        .dimension(new Dimension("$distinct_id", "$distinct_id"))
                        .dimension(new Dimension("$event", "$event"))
                        .dimension(new Dimension("$time", "$time"))
                        .dimension(new Dimension("$device_type", "$device_type"))
                        .dimension(new Dimension("properties.map_id", "properties.map_id"))
                        .dimension(new Dimension("properties.cloud_ssl", "properties.cloud_ssl"))
                        .dimension(new Dimension("properties.error_info", "properties.error_info"))
                        .dimension(new Dimension("properties.error_code", "properties.error_code"))
                        .dimension(new Dimension("properties.$app_version", "properties.$app_version"))
                        .dimension(new Dimension("properties.robotry", "properties.robotry"));
        CubeRegistry.registry((ES_CN + EngineCommon.SPLIT_DVL + tableName).toUpperCase(), cube);
        cube.setCubeName("ES");
        return cube;
    }
}
