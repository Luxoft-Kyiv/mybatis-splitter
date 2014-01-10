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
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Vitalii Tymchyshyn
 */
public class UpdateSplitterPluginTest {
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
        Map<String, Object> param = new HashMap<String, Object>();
        param.put("first", "first");
        param.put("other", Arrays.asList("second", "third"));
        sqlSession.insert("multy", param);
        sqlSession.flushStatements();
        sqlSession.close();
    }
}
