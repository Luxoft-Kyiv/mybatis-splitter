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

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.RowBounds;

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
    private TextSplitter splitter;
    private boolean skipEmptyStatements = true;
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
        return target instanceof Executor
                ? Plugin.wrap(target, new UpdateSplitterPlugin(splitter, skipEmptyStatements))
                : target;
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
        property = properties.getProperty(SKIP_EMPTY_STATEMENTS_PROPERTY);
        if (property != null) {
            skipEmptyStatements = Boolean.parseBoolean(property);
        }
    }

}
