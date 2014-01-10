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

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author Vitalii Tymchyshyn
 */

public class DelimiterSplitter implements TextSplitter{
    private final String delimiter;

    public DelimiterSplitter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Override
    public List<String> split(String text) {
        List<String> splitted = new ArrayList<String>();
        StringTokenizer tokenizer = new StringTokenizer(text, delimiter);
        while(tokenizer.hasMoreTokens()) {
            splitted.add(tokenizer.nextToken().trim());
        }
        return splitted;
    }
}
