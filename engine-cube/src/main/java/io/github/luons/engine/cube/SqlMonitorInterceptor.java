package io.github.luons.engine.cube;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.ResultHandler;

import java.lang.reflect.Field;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

@Slf4j
@SuppressWarnings("unchecked")
@Intercepts(value = {
        @Signature(args = {Statement.class, ResultHandler.class}, method = "query", type = StatementHandler.class),
        @Signature(args = {Statement.class}, method = "update", type = StatementHandler.class),
        @Signature(args = {Statement.class}, method = "batch", type = StatementHandler.class)})
public class SqlMonitorInterceptor implements Interceptor {

    private BoundSql boundSql;

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        Object target = invocation.getTarget();
        long startTime = System.currentTimeMillis();
        StatementHandler statementHandler = (StatementHandler) target;
        try {
            return invocation.proceed();
        } finally {
            boundSql = statementHandler.getBoundSql();
            String sql = boundSql.getSql();
            Object paramObj = boundSql.getParameterObject();
            List<ParameterMapping> paramMappings = boundSql.getParameterMappings();
            // 格式化Sql语句.去除换行符;替换参数
            sql = formatSql(sql, paramObj, paramMappings);
            log.info("query-engine_sql_cost sql={} and cost={}", sql, (System.currentTimeMillis() - startTime));
        }
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
    }

    private String formatSql(String sql, Object paramObj, List<ParameterMapping> paramMappings) {
        if (StringUtils.isBlank(sql)) {
            return "";
        }
        sql = beautifySql(sql);
        if (Objects.isNull(paramObj) || paramMappings == null || paramMappings.isEmpty()) {
            return sql;
        }
        String oldSql = sql;
        // Format SQL
        try {
            Class<?> paramObjectClass = paramObj.getClass();
            if (paramObj instanceof Map) {
                Map<String, Object> paramMap = (Map<String, Object>) paramObj;
                sql = handleMapParameter(sql, paramMap, paramMappings);
            } else {
                sql = handleCommonParameter(sql, paramMappings, paramObjectClass, paramObj);
            }
        } catch (Exception e) {
            return oldSql;
        }
        return sql;
    }

    private String beautifySql(String sql) {
        sql = sql.replaceAll(("[\\s\n ]+"), (" "));
        return sql;
    }

    private boolean isPrimitiveOrPrimitiveWrapper(Class<?> parameterObjectClass) {
        return parameterObjectClass.isPrimitive()
                ||
                (parameterObjectClass.isAssignableFrom(Byte.class)
                        || parameterObjectClass.isAssignableFrom(Short.class)
                        || parameterObjectClass.isAssignableFrom(Integer.class)
                        || parameterObjectClass.isAssignableFrom(Long.class)
                        || parameterObjectClass.isAssignableFrom(Double.class)
                        || parameterObjectClass.isAssignableFrom(Float.class)
                        || parameterObjectClass.isAssignableFrom(Character.class)
                        || parameterObjectClass.isAssignableFrom(Boolean.class));
    }

    private Object getPropertyValue(Map<String, Object> paramMap, String propertyName) {
        Object propertyValue = getParamValue(paramMap, propertyName);
        if (propertyValue != null) {
            return propertyValue;
        }
        if (propertyName.contains(".")) {
            int n = propertyName.indexOf(".");
            String key = propertyName.substring(0, n);
            String subKey = propertyName.substring(n + 1);
            Object paramValue = getParamValue(paramMap, key);
            if (paramValue instanceof Map) {
                return getPropertyValue((Map<String, Object>) paramValue, subKey);
            }
        }
        return null;
    }

    private Object getParamValue(Map<String, Object> paramMap, String propertyName) {
        Object propertyValue;
        try {
            propertyValue = paramMap.get(propertyName);
        } catch (Exception e) {
            log.error("get {} is exception " + e, propertyName);
            propertyValue = boundSql.getAdditionalParameter(propertyName);
        }
        return propertyValue;
    }

    private String handleMapParameter(String sql, Map<String, Object> paramMap, List<ParameterMapping> parameterMappingList) {
        for (ParameterMapping parameterMapping : parameterMappingList) {
            String propertyName = parameterMapping.getProperty();
            Object propertyValue = getPropertyValue(paramMap, propertyName);
            if (propertyValue == null) {
                break;
            }
            if (propertyValue instanceof String) {
                propertyValue = "\"" + propertyValue + "\"";
            }
            sql = sql.replaceFirst(("\\?"), propertyValue.toString());
        }
        return sql;
    }

    private String handleCommonParameter(String sql, List<ParameterMapping> paramMappings,
                                         Class<?> paramObjClass, Object paramObj) throws Exception {
        for (ParameterMapping paramMapping : paramMappings) {
            String propertyValue;
            if (isPrimitiveOrPrimitiveWrapper(paramObjClass)) {
                propertyValue = paramObj.toString();
            } else {
                String propertyName = paramMapping.getProperty();
                Field field = paramObjClass.getDeclaredField(propertyName);
                field.setAccessible(true);
                propertyValue = String.valueOf(field.get(paramObj));
                if (paramMapping.getJavaType().isAssignableFrom(String.class)) {
                    propertyValue = "\"" + propertyValue + "\"";
                }
            }
            sql = sql.replaceFirst(("\\?"), propertyValue);
        }
        return sql;
    }

}
