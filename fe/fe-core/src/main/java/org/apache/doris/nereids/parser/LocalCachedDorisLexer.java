// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.DorisLexer;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.dfa.DFA;

/**
 * LocalCachedDorisLexer
 */
public class LocalCachedDorisLexer extends DorisLexer {
    private final DFA[] localDecisionToDFA;

    /**
     * LocalCachedDorisLexer
     */
    public LocalCachedDorisLexer(CharStream input) {
        super(input);
        localDecisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            localDecisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
        _interp = new LexerATNSimulator(this, _ATN, localDecisionToDFA, _sharedContextCache);
    }
}
