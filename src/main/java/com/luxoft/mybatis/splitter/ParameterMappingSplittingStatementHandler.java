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

import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.session.ResultHandler;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @author Vitalii Tymchyshyn
 */
public class ParameterMappingSplittingStatementHandler implements StatementHandler {
    private final List<ParameterMapping> fullParameterMappings;
    private final StatementHandler delegate;
    private boolean taken = false;

    public ParameterMappingSplittingStatementHandler(List<ParameterMapping> fullParameterMappings, StatementHandler delegate) {
        this.fullParameterMappings = fullParameterMappings;
        this.delegate = delegate;
    }

    @Override
    public Statement prepare(Connection connection) throws SQLException {
        return delegate.prepare(connection);
    }

    @Override
    public void parameterize(Statement statement) throws SQLException {
        if (!taken && statement instanceof PreparedStatement) {
            taken = true;
            int numParams = ((PreparedStatement) statement).getParameterMetaData().getParameterCount();
            List<ParameterMapping> takenParams = fullParameterMappings.subList(0, numParams);
            getBoundSql().getParameterMappings().addAll(takenParams);
            takenParams.clear();
        }
        delegate.parameterize(statement);
    }

    @Override
    public void batch(Statement statement) throws SQLException {
        delegate.batch(statement);
    }

    @Override
    public int update(Statement statement) throws SQLException {
        return delegate.update(statement);
    }

    @Override
    public <E> List<E> query(Statement statement, ResultHandler resultHandler) throws SQLException {
        return delegate.query(statement, resultHandler);
    }

    @Override
    public BoundSql getBoundSql() {
        return delegate.getBoundSql();
    }

    @Override
    public ParameterHandler getParameterHandler() {
        return delegate.getParameterHandler();
    }
}
