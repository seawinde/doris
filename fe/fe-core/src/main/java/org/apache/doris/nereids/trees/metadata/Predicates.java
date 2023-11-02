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

package org.apache.doris.nereids.trees.metadata;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitors.PredicatesSpliter;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.Set;

/**
 * Predicates
 * */
public class Predicates {

    // Predicates that can be pulled up
    private final Set<Expression> pulledUpPredicates;
    // Record the predicates that can not pulled up from outer join or other join
    private final Set<Expression> canNotPulledUpPredicates;

    public Predicates(Set<Expression> pulledUpPredicates, Set<Expression> canNotPulledUpPredicates) {
        this.pulledUpPredicates = pulledUpPredicates;
        this.canNotPulledUpPredicates = canNotPulledUpPredicates;
    }

    public static Predicates of(Set<Expression> pulledUpPredicates,
            Set<Expression> canNotPulledUpPredicates) {
        return new Predicates(pulledUpPredicates, canNotPulledUpPredicates);
    }

    public Set<Expression> getPulledUpPredicates() {
        return pulledUpPredicates;
    }

    public Set<Expression> getCanNotPulledUpPredicates() {
        return canNotPulledUpPredicates;
    }

    public Expression composedExpression() {
        return ExpressionUtils.and(ExpressionUtils.and(pulledUpPredicates),
                ExpressionUtils.and(canNotPulledUpPredicates));
    }

    /**
     * SplitPredicate
     * */
    public static SplitPredicate splitPredicates(Expression expression) {
        PredicatesSpliter predicatesSplit = new PredicatesSpliter();
        expression.accept(predicatesSplit, null);
        return predicatesSplit.getSplitPredicate();
    }

    /**
     * SplitPredicate
     * */
    public static final class SplitPredicate {
        private final Expression equalPredicates;
        private final Expression rangePredicates;
        private final Expression residualPredicates;

        public SplitPredicate(Expression equalPredicates, Expression rangePredicates, Expression residualPredicates) {
            this.equalPredicates = equalPredicates;
            this.rangePredicates = rangePredicates;
            this.residualPredicates = residualPredicates;
        }

        public Expression getEqualPredicates() {
            return equalPredicates;
        }

        public Expression getRangePredicates() {
            return rangePredicates;
        }

        public Expression getResidualPredicates() {
            return residualPredicates;
        }

        public static SplitPredicate empty() {
            return new SplitPredicate(null, null, null);
        }

        /**
         * SplitPredicate
         * */
        public static SplitPredicate of(Expression equalPredicates,
                Expression rangePredicates,
                Expression residualPredicates) {
            return new SplitPredicate(equalPredicates, rangePredicates, residualPredicates);
        }

        /**
         * isEmpty
         * */
        public boolean isEmpty() {
            return equalPredicates == null
                    && rangePredicates == null
                    && residualPredicates == null;
        }

        public Expression composedExpression() {
            return ExpressionUtils.and(equalPredicates, rangePredicates, residualPredicates);
        }
    }
}
