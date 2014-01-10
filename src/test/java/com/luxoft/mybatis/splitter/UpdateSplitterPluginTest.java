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

import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.unitils.UnitilsJUnit4TestClassRunner;
import org.unitils.easymock.annotation.Mock;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.unitils.easymock.EasyMockUnitils.replay;

/**
 * @author Vitalii Tymchyshyn
 */
@RunWith(UnitilsJUnit4TestClassRunner.class)
public class UpdateSplitterPluginTest {
    @Mock
    Connection connection;

    @Mock
    PreparedStatement statement;

    @Test
    public void splitterTestSimple() throws IOException, SQLException {
        splitterTest(ExecutorType.SIMPLE);
    }

    @Test
    public void splitterTestBatch() throws IOException, SQLException {
        splitterTest(ExecutorType.BATCH);
    }

    public void splitterTest(ExecutorType execType) throws IOException, SQLException {
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(
                Resources.getResourceAsStream("configuration.xml"));
        SqlSession sqlSession = sqlSessionFactory.openSession(execType);
        sqlSession.insert("makeTable");
        sqlSession.flushStatements();
        doInsert(sqlSession);
        Assert.assertEquals(Arrays.asList("first", "second", "third"), sqlSession.selectList("get"));
        sqlSession.insert("dropTable");
        sqlSession.flushStatements();
        sqlSession.close();
    }

    private void doInsert(SqlSession sqlSession) {
        Map<String, Object> param = new HashMap<String, Object>();
        param.put("first", "first");
        param.put("other", Arrays.asList("second", "third"));
        sqlSession.insert("multy", param);
        sqlSession.flushStatements();
    }

    @Test
    public void mockTest() throws IOException, SQLException {
        expect(connection.getAutoCommit()).andStubReturn(false);
        expect(connection.prepareStatement("insert into test values(?)")).andReturn(statement);
        statement.setString(1, "first");
        statement.addBatch();
        expect(connection.prepareStatement("insert into test values(?)")).andReturn(statement);
        statement.setString(1, "second");
        statement.addBatch();
        expect(connection.prepareStatement("insert into test values(?)")).andReturn(statement);
        statement.setString(1, "third");
        statement.addBatch();
        expect(statement.executeBatch()).andStubReturn(new int[]{1});
        statement.close();
        statement.close();
        statement.close();
        connection.setAutoCommit(true);
        connection.rollback();
        connection.close();

        replay();

        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(
                Resources.getResourceAsStream("configuration.xml"));
        SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, connection);
        doInsert(sqlSession);
        sqlSession.close();
    }
}
