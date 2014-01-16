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

import org.apache.ibatis.executor.BaseExecutor;
import org.apache.ibatis.executor.BatchExecutorException;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Based on {@link org.apache.ibatis.executor.BatchExecutor}
 *
 * @author Vitalii Tymchyshyn
 */
public class ReusingBatchExecutor extends BaseExecutor {

    public static final int BATCH_UPDATE_RETURN_VALUE = Integer.MIN_VALUE + 1002;

    private final boolean retainExecuteOrder;
    private final boolean reuseBetweenFlushes;
    private final Map<PreparedStatementKey, StatementData> statementsData = new LinkedHashMap<PreparedStatementKey, StatementData>();
    private final Map<PreparedStatementKey, StatementData> unusedStatementData = new HashMap<PreparedStatementKey, StatementData>();
    private PreparedStatementKey lastKey;
    private List<BatchResult> results = new ArrayList<BatchResult>();

    public ReusingBatchExecutor(Configuration configuration, Transaction transaction, boolean retainExecuteOrder, boolean reuseBetweenFlushes) {
        super(configuration, transaction);
        this.retainExecuteOrder = retainExecuteOrder;
        this.reuseBetweenFlushes = reuseBetweenFlushes;
    }

    public int doUpdate(MappedStatement ms, Object parameterObject) throws SQLException {
        final Configuration configuration = ms.getConfiguration();
        final StatementHandler handler = configuration.newStatementHandler(this, ms, parameterObject, RowBounds.DEFAULT, null, null);
        final BoundSql boundSql = handler.getBoundSql();
        PreparedStatementKey key = new PreparedStatementKey(boundSql.getSql(), ms);
        StatementData statementData = statementsData.get(key);
        if (retainExecuteOrder && statementData != null && !key.equals(lastKey)) {
            statementData = null;
            executeUpTo(key, true);
        }
        if (statementData == null) {
            statementData = unusedStatementData.remove(key);
            if (statementData == null) {
                Connection connection = getConnection(ms.getStatementLog());
                Statement stmt = handler.prepare(connection);
                statementData = new StatementData(stmt);
            }
            statementsData.put(key, statementData);
        }
        lastKey = key;
        statementData.addParameterObject(parameterObject);
        handler.parameterize(statementData.getStatement());
        handler.batch(statementData.getStatement());
        return BATCH_UPDATE_RETURN_VALUE;
    }

    public <E> List<E> doQuery(MappedStatement ms, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql)
            throws SQLException {
        Statement stmt = null;
        try {
            flushStatements();
            Configuration configuration = ms.getConfiguration();
            StatementHandler handler = configuration.newStatementHandler(this, ms, parameterObject, rowBounds, resultHandler, boundSql);
            Connection connection = getConnection(ms.getStatementLog());
            stmt = handler.prepare(connection);
            handler.parameterize(stmt);
            return handler.<E>query(stmt, resultHandler);
        } finally {
            closeStatement(stmt);
        }
    }

    public List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException {
        try {
            if (isRollback) {
                return Collections.emptyList();
            } else {
                return executeStatements();
            }
        } finally {
            for (StatementData stmt : statementsData.values()) {
                closeStatement(stmt.getStatement());
            }
            if (!reuseBetweenFlushes) {
                for (StatementData stmt : unusedStatementData.values()) {
                    closeStatement(stmt.getStatement());
                }
            }
            lastKey = null;
            statementsData.clear();
            unusedStatementData.clear();
        }
    }

    private List<BatchResult> executeStatements() throws SQLException {
        executeUpTo(null, false);
        List<BatchResult> batchResults = results;
        results = new ArrayList<BatchResult>();
        return batchResults;
    }

    private void executeUpTo(PreparedStatementKey lastToExecute, boolean moveToReuse) throws SQLException {
        for (Iterator<Map.Entry<PreparedStatementKey, StatementData>> iterator = statementsData.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<PreparedStatementKey, StatementData> entry = iterator.next();
            StatementData statementData = entry.getValue();
            Statement stmt = statementData.getStatement();
            BatchResult batchResult = new BatchResult(entry.getKey().getMappedStatement(), entry.getKey().getSql());
            batchResult.getParameterObjects().addAll(entry.getValue().getParameterObjects());
            try {
                batchResult.setUpdateCounts(stmt.executeBatch());
                MappedStatement ms = entry.getKey().getMappedStatement();
                List<Object> parameterObjects = statementData.getParameterObjects();
                KeyGenerator keyGenerator = ms.getKeyGenerator();
                if (keyGenerator instanceof Jdbc3KeyGenerator) {
                    Jdbc3KeyGenerator jdbc3KeyGenerator = (Jdbc3KeyGenerator) keyGenerator;
                    jdbc3KeyGenerator.processBatch(ms, stmt, parameterObjects);
                } else {
                    for (Object parameter : parameterObjects) {
                        keyGenerator.processAfter(this, ms, stmt, parameter);
                    }
                }
            } catch (BatchUpdateException e) {
                List<BatchResult> batchResults = results;
                results = new ArrayList<BatchResult>();
                throw new BatchExecutorException(
                        entry.getKey().getMappedStatement().getId() +
                                " (batch query " + entry.getKey().getSql() + ")" +
                                " failed. Prior " +
                                batchResults.size() + " queries completed successfully, but will be rolled back.",
                        e, batchResults, batchResult);
            }
            results.add(batchResult);
            if (moveToReuse) {
                iterator.remove();
                unusedStatementData.put(entry.getKey(), statementData);
            }
            if (entry.getKey().equals(lastToExecute)) {
                break;
            }
        }
    }

    public static class PreparedStatementKey {
        private final String sql;
        private final MappedStatement mappedStatement;
        private final int hashCode;

        protected PreparedStatementKey(String sql, MappedStatement mappedStatement) {
            this.sql = sql;
            this.mappedStatement = mappedStatement;
            hashCode = sql.hashCode() * 31 + mappedStatement.hashCode();
        }

        public String getSql() {
            return sql;
        }

        public MappedStatement getMappedStatement() {
            return mappedStatement;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PreparedStatementKey that = (PreparedStatementKey) o;

            if (!sql.equals(that.sql)) return false;
            if (!mappedStatement.equals(that.mappedStatement)) return false;

            return true;
        }
    }

    public static class StatementData {
        private final Statement statement;
        private final List<Object> parameterObjects = new ArrayList<Object>();

        public StatementData(Statement statement) {
            this.statement = statement;
        }

        public Statement getStatement() {
            return statement;
        }

        public void addParameterObject(Object parameterObject) {
            parameterObjects.add(parameterObject);
        }

        public List<Object> getParameterObjects() {
            return parameterObjects;
        }
    }

}
