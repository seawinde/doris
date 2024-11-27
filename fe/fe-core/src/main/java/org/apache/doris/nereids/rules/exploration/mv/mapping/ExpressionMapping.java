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

package org.apache.doris.nereids.rules.exploration.mv.mapping;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Expression mapping, maybe one expression map to multi expression
 */
public class ExpressionMapping extends Mapping {

    public static final ExpressionMapping EMPTY_INSTANCE =
            ExpressionMapping.generate(ImmutableList.of(), ImmutableList.of());
    private final Multimap<Expression, Expression> expressionMapping;

    public ExpressionMapping(Multimap<Expression, Expression> expressionMapping) {
        this.expressionMapping = expressionMapping;
    }

    public Multimap<Expression, Expression> getExpressionMapping() {
        return expressionMapping;
    }

    /**
     * ExpressionMapping flatten
     */
    public List<Map<Expression, Expression>> flattenMap() {
        List<List<Pair<Expression, Expression>>> tmpExpressionPairs = new ArrayList<>(this.expressionMapping.size());
        Map<? extends Expression, ? extends Collection<? extends Expression>> expressionMappingMap =
                expressionMapping.asMap();
        for (Map.Entry<? extends Expression, ? extends Collection<? extends Expression>> entry
                : expressionMappingMap.entrySet()) {
            List<Pair<Expression, Expression>> targetExpressionList = new ArrayList<>(entry.getValue().size());
            for (Expression valueExpression : entry.getValue()) {
                targetExpressionList.add(Pair.of(entry.getKey(), valueExpression));
            }
            tmpExpressionPairs.add(targetExpressionList);
        }
        List<List<Pair<Expression, Expression>>> cartesianExpressionMap = Lists.cartesianProduct(tmpExpressionPairs);

        final List<Map<Expression, Expression>> flattenedMap = new ArrayList<>();
        for (List<Pair<Expression, Expression>> listPair : cartesianExpressionMap) {
            final Map<Expression, Expression> expressionMap = new HashMap<>();
            listPair.forEach(pair -> expressionMap.put(pair.key(), pair.value()));
            flattenedMap.add(expressionMap);
        }
        return flattenedMap;
    }

    /**
     * Permute the key of expression mapping. this is useful for expression rewrite, if permute key to query based
     * then when expression rewrite success, we can get the mv scan expression directly.
     */
    public ExpressionMapping keyPermute(SlotMapping slotMapping) {
        Multimap<Expression, Expression> permutedExpressionMapping = ArrayListMultimap.create();
        Map<? extends Expression, ? extends Collection<? extends Expression>> expressionMap =
                this.getExpressionMapping().asMap();
        for (Map.Entry<? extends Expression, ? extends Collection<? extends Expression>> entry :
                expressionMap.entrySet()) {
            Expression replacedExpr = ExpressionUtils.replace(entry.getKey(), slotMapping.toSlotReferenceMap());
            permutedExpressionMapping.putAll(replacedExpr, entry.getValue());
        }
        return new ExpressionMapping(permutedExpressionMapping);
    }

    /**
     * ExpressionMapping generate
     */
    public static ExpressionMapping generate(
            List<? extends Expression> sourceExpressions,
            List<? extends Expression> targetExpressions) {
        final Multimap<Expression, Expression> expressionMultiMap =
                ArrayListMultimap.create();
        for (int i = 0; i < sourceExpressions.size(); i++) {
            expressionMultiMap.put(sourceExpressions.get(i), targetExpressions.get(i));
        }
        return new ExpressionMapping(expressionMultiMap);
    }

    /**
     * Generate expression mapping, just map the first element in queryShuttledExprToExprMap and
     * viewShuttledExprToExprMap
     * Such as
     * queryShuttledExprToExprMap is {c1#0 : [c1#1, c1#2]}
     * viewShuttledExprToExprMap is {c1#3 : [c1#4, c1#4]}
     * if viewToQuerySlotMapping is {c1#0 : c1#3}
     * the mapping result is {c1#1 : c1#4}
     * */
    public static BiMap<Expression, Expression> generateExpressionMappingFirst(
            Map<SlotReference, SlotReference> viewToQuerySlotMapping,
            Multimap<Expression, Expression> queryShuttledExprToExprMap,
            Multimap<Expression, Expression> viewShuttledExprToExprMap) {
        final Map<Expression, Expression> viewEdgeToConjunctsMapQueryBased = new HashMap<>();
        BiMap<Expression, Expression> queryToViewEdgeMapping = HashBiMap.create();
        if (queryShuttledExprToExprMap == null || viewShuttledExprToExprMap == null
                || queryShuttledExprToExprMap.isEmpty() || viewShuttledExprToExprMap.isEmpty()) {
            return queryToViewEdgeMapping;
        }
        viewShuttledExprToExprMap.forEach((shuttledExpr, expr) -> {
            viewEdgeToConjunctsMapQueryBased.put(
                    orderSlotAsc(ExpressionUtils.replace(shuttledExpr, viewToQuerySlotMapping)), expr);
        });
        queryShuttledExprToExprMap.forEach((exprSet, edge) -> {
            Expression viewExpr = viewEdgeToConjunctsMapQueryBased.get(orderSlotAsc(exprSet));
            if (viewExpr != null) {
                queryToViewEdgeMapping.putIfAbsent(edge, viewExpr);
            }
        });
        return queryToViewEdgeMapping;
    }

    /**
     * Generate expression mapping, map the unique combine element in queryShuttledExprToExprMap and
     * viewShuttledExprToExprMap
     * Such as
     * queryShuttledExprToExprMap is {c1#0 : [c1#1, c1#2]}
     * viewShuttledExprToExprMap is {c1#3 : [c1#4, c1#5]}
     * if viewToQuerySlotMapping is {c1#0 : c1#3}
     * the mapping result is {c1#1 : c1#4, c1#2 : c1#5}
     * */
    public List<BiMap<Expression, Expression>> generateExpressionMappingCombine(
            Map<SlotReference, SlotReference> viewToQuerySlotMapping,
            Multimap<Expression, Expression> queryShuttledExprToExprMap,
            Multimap<Expression, Expression> viewShuttledExprToExprMap) {
        List<BiMap<Expression, Expression>> result = new ArrayList<>();
        if (queryShuttledExprToExprMap == null || viewShuttledExprToExprMap == null
                || queryShuttledExprToExprMap.isEmpty() || viewShuttledExprToExprMap.isEmpty()) {
            return result;
        }

        final Multimap<Expression, Expression> viewEdgeToConjunctsMapQueryBased = HashMultimap.create();
        viewShuttledExprToExprMap.forEach((shuttledExpr, expr) -> {
            viewEdgeToConjunctsMapQueryBased.put(
                    orderSlotAsc(ExpressionUtils.replace(shuttledExpr, viewToQuerySlotMapping)), expr);
        });

        List<List<BiMap<Expression, Expression>>> expressionMappingList = new ArrayList<>();
        for (Entry<Expression, Collection<Expression>> queryEntry : queryShuttledExprToExprMap.asMap().entrySet()) {
            Collection<Expression> viewExpressions = viewEdgeToConjunctsMapQueryBased.get(
                    orderSlotAsc(queryEntry.getKey()));
            if (queryEntry.getValue().size() == 1 && viewExpressions.size() == 1) {
                HashBiMap<Expression, Expression> expressionMapping = HashBiMap.create();
                expressionMapping.put(queryEntry.getValue().iterator().next(), viewExpressions.iterator().next());
                expressionMappingList.add(ImmutableList.of(expressionMapping));
                continue;
            }
            List<BiMap<Expression, Expression>> permutationMappings = getUniquePermutation(
                    queryEntry.getValue().toArray(new Expression[0]),
                    viewExpressions.toArray(new Expression[0]), Expression.class);
            expressionMappingList.add(permutationMappings);
        }
        return Lists.cartesianProduct(expressionMappingList).stream()
                .map(listMapping -> {
                    if (listMapping.size() == 1) {
                        return listMapping.get(0);
                    }
                    HashBiMap<Expression, Expression> expressionMapping = HashBiMap.create();
                    for (BiMap<Expression, Expression> expressionBiMap : listMapping) {
                        expressionMapping.putAll(expressionBiMap);
                    }
                    return expressionMapping;
                })
                .collect(ImmutableList.toImmutableList());
    }

    private static Expression orderSlotAsc(Expression expression) {
        return expression.accept(ExpressionSlotOrder.INSTANCE, null);
    }

    private static final class ExpressionSlotOrder extends DefaultExpressionRewriter<Void> {
        public static final ExpressionSlotOrder INSTANCE = new ExpressionSlotOrder();

        @Override
        public Expression visitEqualTo(EqualTo equalTo, Void context) {
            if (!(equalTo.getArgument(0) instanceof NamedExpression)
                    || !(equalTo.getArgument(1) instanceof NamedExpression)) {
                return equalTo;
            }
            NamedExpression left = (NamedExpression) equalTo.getArgument(0);
            NamedExpression right = (NamedExpression) equalTo.getArgument(1);
            if (right.getExprId().asInt() < left.getExprId().asInt()) {
                return new EqualTo(right, left);
            } else {
                return equalTo;
            }
        }
    }

    @Override
    public String toString() {
        return Utils.toSqlString("ExpressionMapping", "expressionMapping", expressionMapping);
    }
}
