/*
 *  Copyright 2014 Vitalii Tymchyshyn
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.luxoft.mybatis.splitter;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.Configuration;

import java.util.List;

/**
* @author Vitalii Tymchyshyn
*/
class SwitchingSqlSource implements SqlSource {
    private final Configuration configuration;
    private String sql;
    private List<ParameterMapping> parameterMappings;
    private BoundSql parentBoundSql;

    SwitchingSqlSource(Configuration configuration) {
        this.configuration = configuration;
    }

    public SwitchingSqlSource(Configuration configuration, String sql, BoundSql boundSql, List<ParameterMapping> parameterMappings) {
        this.sql = sql;
        this.parameterMappings = parameterMappings;
        this.configuration = configuration;
        this.parentBoundSql = boundSql;
    }

    @Override
    public BoundSql getBoundSql(Object parameterObject) {
        BoundSql subBoundSql = new BoundSql(configuration, sql, parameterMappings, parameterObject);
        for (ParameterMapping parameterMapping: subBoundSql.getParameterMappings()) {
            String property = parameterMapping.getProperty();
            if (parentBoundSql.hasAdditionalParameter(property)) {
                subBoundSql.setAdditionalParameter(property, parentBoundSql.getAdditionalParameter(property));
            }
        }
        return subBoundSql;
    }

    public void switchParams(String sql, BoundSql parentBoundSql, List<ParameterMapping> parameterMappings) {

        this.sql = sql;
        this.parentBoundSql = parentBoundSql;
        this.parameterMappings = parameterMappings;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public void setParameterMappings(List<ParameterMapping> parameterMappings) {
        this.parameterMappings = parameterMappings;
    }

    public void setParentBoundSql(BoundSql parentBoundSql) {
        this.parentBoundSql = parentBoundSql;
    }
}
