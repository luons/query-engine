package com.luons.engine.demo;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.ninebot.bigdata.query.cube.SqlMonitorInterceptor;
import com.ninebot.bigdata.query.cube.mapper.CommonMysqlMapper;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.session.SqlSession;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Lazy
@Configuration
@ComponentScan(basePackages = {"com.luons.engine.cube"})
@PropertySources(value = {@PropertySource({"classpath:model.properties"})})
public class CubeContext {

    @Autowired
    public Environment env;

    @Bean(name = "cubeMysqlDatasource")
    public DataSource cubeMysqlDatasource() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setDriverClassName(env.getProperty("mysql.jdbc.driverClassName"));
        druidDataSource.setUrl(env.getProperty("mysql.jdbc.url"));
        druidDataSource.setUsername(env.getProperty("mysql.jdbc.username"));
        druidDataSource.setPassword(env.getProperty("mysql.jdbc.password"));
        druidDataSource.setDefaultAutoCommit(false);
        druidDataSource.setInitialSize(5);
        druidDataSource.setMinIdle(5);
        druidDataSource.setMaxActive(10);
        return druidDataSource;
    }

    @Bean(name = "cubeMysqlSessionFactoryBean")
    public SqlSessionFactoryBean cubeMysqlSessionFactoryBean() throws IOException {
        SqlSessionFactoryBean sqlSessionFactoryBean = new SqlSessionFactoryBean();
        sqlSessionFactoryBean.setDataSource(cubeMysqlDatasource());
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] mapperLocations = resolver.getResources("classpath:mybatis/**/*.xml");
        sqlSessionFactoryBean.setMapperLocations(mapperLocations);
        sqlSessionFactoryBean.setPlugins(new Interceptor[]{new SqlMonitorInterceptor()});
        return sqlSessionFactoryBean;
    }

    @Bean(name = "cubeMysqlSessionTemplate")
    public SqlSession cubeMysqlSessionTemplate() throws Exception {
        return new SqlSessionTemplate(Objects.requireNonNull(cubeMysqlSessionFactoryBean().getObject()));
    }

    @Bean(name = "commonMysqlMapper")
    public CommonMysqlMapper commonMysqlMapper() throws Exception {
        return cubeMysqlSessionTemplate().getMapper(CommonMysqlMapper.class);
    }

    @Bean
    public ThreadPoolExecutor cubeQueryThreadPool() {
        return new ThreadPoolExecutor(env.getProperty("cube.query.thread.pool.corePoolSize",
                Integer.class, 100), env.getProperty("cube.query.thread.pool.maximumPoolSize",
                Integer.class, 100), env.getProperty("cube.query.thread.pool.keepAliveTime", Long.class,
                60L), TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000), new ThreadFactoryBuilder()
                .setNameFormat("cube-query-thread-%d").build(), new ThreadPoolExecutor.AbortPolicy());
    }

}
