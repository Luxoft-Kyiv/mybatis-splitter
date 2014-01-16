/*
   Copyright 2014 Luxoft

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


 */
package com.luxoft.mybatis.splitter;

import org.apache.ibatis.executor.*;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.RowBounds;

import java.lang.reflect.Field;
import java.util.*;

/**
 * @author Vitalii Tymchyshyn
 */
@Intercepts({@Signature(
        type = Executor.class,
        method= "update",
        args = {MappedStatement.class, Object.class}
)})
public class UpdateSplitterPlugin implements Interceptor{
    public static final String SPLIT_EXPRESSION_PROPERTY = "splitExpression";
    public static final String DELIMITER_PROPERTY = "delimiter";
    public static final String SKIP_EMPTY_STATEMENTS_PROPERTY = "skipEmptyStatements";
    public static final String REUSE_PREPARED_STATEMENTS_PROPERTY = "reusePreparedStatements";
    public static final String REUSE_BETWEEN_FLUSHES_PROPERTY = "reuseBetweenFlushes";
    public static final String RETAIN_EXECUTE_ORDER_PROPERTY = "retainExecuteOrder";
    public static final String MSG_ERROR_ACCESSING_CONFIGURATION = "Can't access executor configuration field. Please set reusePreparedStatements to false";
    public static final String MSG_ERROR_ACCESSING_DELEGATE = "Can't access executor delegate field. Please set reusePreparedStatements to false";
    private TextSplitter splitter;
    private boolean skipEmptyStatements = true;
    private boolean reusePreparedStatements = true;
    private boolean reuseBetweenFlushes = false;
    private boolean retainExecuteOrder = false;
    private Field executorConfiguration;
    private Field cachingExecutorDelegate;
    private Map<MappedStatement, MappedStatement> subStatements = new HashMap<MappedStatement, MappedStatement>();

    public UpdateSplitterPlugin() {
        this(new DelimiterSplitter(";"));
    }

    public UpdateSplitterPlugin(TextSplitter splitter) {
        this.splitter = splitter;
    }

    public UpdateSplitterPlugin(TextSplitter splitter, boolean skipEmptyStatements) {
        this.splitter = splitter;
        this.skipEmptyStatements = skipEmptyStatements;
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        MappedStatement ms = (MappedStatement) invocation.getArgs()[0];
        Object parameterObject = invocation.getArgs()[1];
        final Configuration configuration = ms.getConfiguration();
        final StatementHandler handler = configuration.newStatementHandler((Executor) invocation.getTarget(),
                ms, parameterObject, RowBounds.DEFAULT, null, null);
        final BoundSql boundSql = handler.getBoundSql();
        final String sql = boundSql.getSql();
        List<String> splitted = splitter.split(sql);
        int rc = 0;
        List<ParameterMapping> fullParameterMappings = new ArrayList<ParameterMapping>(boundSql.getParameterMappings());
        for (String sqlPart: splitted) {
            if (skipEmptyStatements && sqlPart.length() == 0) {
                continue;
            }
            int numParams = 0;
            for (int index = sqlPart.indexOf('?'); index >=0; index = sqlPart.indexOf('?', index + 1)) {
                numParams++;
            }
            MappedStatement subStatement = subStatements.get(ms);
            if (subStatement == null) {
                subStatement = new MappedStatement.Builder(
                    ms.getConfiguration(), ms.getId(), new SwitchingSqlSource(configuration), ms.getSqlCommandType())
                    .cache(ms.getCache())
                    .databaseId(ms.getDatabaseId())
                    .fetchSize(ms.getFetchSize())
                    .timeout(ms.getTimeout())
                    .flushCacheRequired(ms.isFlushCacheRequired())
                    .useCache(ms.isUseCache())
                    .build();
                subStatements.put(ms, subStatement);
            }
            List<ParameterMapping> subParameterMappings = fullParameterMappings.subList(0, numParams);
            ((SwitchingSqlSource)subStatement.getSqlSource()).switchParams(sqlPart, boundSql,
                    new ArrayList<ParameterMapping>(subParameterMappings));
            subParameterMappings.clear();
            int subRc = (Integer)invocation.getMethod().invoke(invocation.getTarget(), subStatement, parameterObject);
            if (rc >= 0) {
                rc = subRc < 0 ? subRc : rc + subRc;
            }
        }
        return rc;
    }

    @Override
    public Object plugin(Object target) {
        if (reusePreparedStatements && target instanceof BatchExecutor) {
            target = replaceBatchExecutor((BatchExecutor) target);
        }
        if (reusePreparedStatements && target instanceof CachingExecutor) {
            try {
                Object delegate = cachingExecutorDelegate.get(target);
                if (delegate instanceof BatchExecutor) {
                    cachingExecutorDelegate.set(target, replaceBatchExecutor((BatchExecutor) delegate));
                }
            } catch (IllegalAccessException e) {
                throw new ExecutorException(MSG_ERROR_ACCESSING_DELEGATE, e);
            }
        }
        return target instanceof Executor
                ? Plugin.wrap(target, new UpdateSplitterPlugin(splitter, skipEmptyStatements))
                : target;
    }

    private Object replaceBatchExecutor(BatchExecutor target) {
        try {
            return new ReusingBatchExecutor((Configuration) executorConfiguration.get(target),
                    target.getTransaction(), retainExecuteOrder, reuseBetweenFlushes);
        } catch (IllegalAccessException e) {
            throw new ExecutorException(MSG_ERROR_ACCESSING_CONFIGURATION, e);
        }
    }

    @Override
    public void setProperties(Properties properties) {
        String property = properties.getProperty(SPLIT_EXPRESSION_PROPERTY);
        if (property != null) {
            splitter = new RegexpSplitter(property);
        }
        property = properties.getProperty(DELIMITER_PROPERTY);
        if (property != null) {
            splitter = new DelimiterSplitter(property);
        }
        skipEmptyStatements = getBooleanProperty(properties, SKIP_EMPTY_STATEMENTS_PROPERTY, skipEmptyStatements);
        retainExecuteOrder = getBooleanProperty(properties, RETAIN_EXECUTE_ORDER_PROPERTY, retainExecuteOrder);
        reusePreparedStatements = getBooleanProperty(properties, REUSE_PREPARED_STATEMENTS_PROPERTY, reusePreparedStatements);
        reuseBetweenFlushes = getBooleanProperty(properties, REUSE_BETWEEN_FLUSHES_PROPERTY, reuseBetweenFlushes);
        if (reusePreparedStatements) {
            try {
                executorConfiguration = BaseExecutor.class.getDeclaredField("configuration");
                executorConfiguration.setAccessible(true);
            } catch (Exception e) {
                throw new ExecutorException(MSG_ERROR_ACCESSING_CONFIGURATION, e);
            }
            try {
                cachingExecutorDelegate = CachingExecutor.class.getDeclaredField("delegate");
                cachingExecutorDelegate.setAccessible(true);
            } catch (Exception e) {
                throw new ExecutorException(MSG_ERROR_ACCESSING_DELEGATE, e);
            }
        }
    }

    private boolean getBooleanProperty(Properties properties, String name, boolean def) {
        String property;
        property = properties.getProperty(name);
        return property != null ? Boolean.parseBoolean(property) : def;
    }

}
