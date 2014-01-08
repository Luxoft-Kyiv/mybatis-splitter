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

import org.apache.ibatis.builder.StaticSqlSource;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.RowBounds;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

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
    public static final Pattern PARAMETER_COUNTER_REGEXP = Pattern.compile("[^?]*");
    private TextSplitter splitter;

    public UpdateSplitterPlugin() {
        this(new RegexpSplitter("\\s*;\\s*"));
    }

    public UpdateSplitterPlugin(TextSplitter splitter) {
        this.splitter = splitter;
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
        String[] splitted = splitter.split(sql);
        int rc = 0;
        List<ParameterMapping> fullParameterMappings = new ArrayList<ParameterMapping>(boundSql.getParameterMappings());
        for (String sqlPart: splitted) {
            int numParams = PARAMETER_COUNTER_REGEXP.matcher(sqlPart).replaceAll("").length();
            List<ParameterMapping> subParameterMappings = fullParameterMappings.subList(0, numParams);
            SqlSource subSource = new StaticSqlSource(ms.getConfiguration(), sqlPart, new ArrayList<ParameterMapping>(subParameterMappings)) {
                @Override
                public BoundSql getBoundSql(Object parameterObject) {
                    BoundSql subBoundSql = super.getBoundSql(parameterObject);
                    for (ParameterMapping parameterMapping: subBoundSql.getParameterMappings()) {
                        String property = parameterMapping.getProperty();
                        if (boundSql.hasAdditionalParameter(property)) {
                            subBoundSql.setAdditionalParameter(property, boundSql.getAdditionalParameter(property));
                        }
                    }
                    return subBoundSql;
                }
            };
            subParameterMappings.clear();
            MappedStatement subStatement = new MappedStatement.Builder(
                    ms.getConfiguration(), ms.getId(), subSource, ms.getSqlCommandType())
                    .cache(ms.getCache())
                    .databaseId(ms.getDatabaseId())
                    .fetchSize(ms.getFetchSize())
                    .timeout(ms.getTimeout())
                    .flushCacheRequired(ms.isFlushCacheRequired())
                    .useCache(ms.isUseCache())
                    .build();
            int subRc = (Integer)invocation.getMethod().invoke(invocation.getTarget(), subStatement, parameterObject);
            if (rc >= 0) {
                rc = subRc < 0 ? subRc : rc + subRc;
            }
        }
        return rc;
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties properties) {
        String splitExpression = properties.getProperty(SPLIT_EXPRESSION_PROPERTY);
        if (splitExpression != null) {
            splitter = new RegexpSplitter(splitExpression);
        }
    }
}
