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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo.ExpressionPosition;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.Mapping.MappedRelation;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Suppliers;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * For outer join we should check the outer join compatibility between query and view
 */
public class LogicalCompatibilityContext {
    private final BiMap<StructInfoNode, StructInfoNode> queryToViewNodeMapping;
    private final BiMap<Integer, Integer> queryToViewNodeIDMapping;
    private final ObjectId planNodeId;
    private final Supplier<BiMap<Expression, Expression>> queryToViewJoinEdgeExpressionMappingSupplier;
    private final Supplier<BiMap<Expression, Expression>> queryToViewNodeExpressionMappingSupplier;
    private final Supplier<BiMap<Expression, Expression>> queryToViewFilterEdgeExpressionMappingSupplier;
    private final List<BiMap<Expression, Expression>> queryToViewFilterEdgeExpressionMappingList;

    private LogicalCompatibilityContext(
            BiMap<StructInfoNode, StructInfoNode> queryToViewNodeMapping,
            BiMap<Integer, Integer> queryToViewNodeIDMapping,
            ObjectId planNodeId,
            BiMap<Expression, Expression> queryToViewJoinEdgeExpressionMapping,
            BiMap<Expression, Expression> queryToViewNodeExpressionMapping,
            BiMap<Expression, Expression> queryToViewFilterEdgeExpressionMapping,
            List<BiMap<Expression, Expression>> queryToViewFilterEdgeExpressionMappingList) {
        this.queryToViewNodeMapping = queryToViewNodeMapping;
        this.queryToViewNodeIDMapping = queryToViewNodeIDMapping;
        this.planNodeId = planNodeId;
        this.queryToViewJoinEdgeExpressionMappingSupplier =
                Suppliers.memoize(() -> queryToViewJoinEdgeExpressionMapping);
        this.queryToViewNodeExpressionMappingSupplier =
                Suppliers.memoize(() -> queryToViewNodeExpressionMapping);
        this.queryToViewFilterEdgeExpressionMappingSupplier =
                Suppliers.memoize(() -> queryToViewFilterEdgeExpressionMapping);
        this.queryToViewFilterEdgeExpressionMappingList = queryToViewFilterEdgeExpressionMappingList;
    }

    /**
     * LogicalCompatibilityContext
     */
    private LogicalCompatibilityContext(BiMap<StructInfoNode, StructInfoNode> queryToViewNodeMapping,
            Map<SlotReference, SlotReference> viewToQuerySlotMapping, StructInfo queryStructInfo,
            StructInfo viewStructInfo) {

        this.queryToViewJoinEdgeExpressionMappingSupplier =
                Suppliers.memoize(() -> ExpressionMapping.generateExpressionMappingFirst(viewToQuerySlotMapping,
                        queryStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.JOIN_EDGE),
                        viewStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.JOIN_EDGE)));

        this.queryToViewNodeExpressionMappingSupplier =
                Suppliers.memoize(() -> ExpressionMapping.generateExpressionMappingFirst(viewToQuerySlotMapping,
                        queryStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.NODE),
                        viewStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.NODE)));

        this.queryToViewFilterEdgeExpressionMappingSupplier =
                Suppliers.memoize(() -> ExpressionMapping.generateExpressionMappingFirst(viewToQuerySlotMapping,
                        queryStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.FILTER_EDGE),
                        viewStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.FILTER_EDGE)));

        this.queryToViewNodeMapping = queryToViewNodeMapping;
        this.queryToViewNodeIDMapping = HashBiMap.create();
        queryToViewNodeMapping.forEach((k, v) -> queryToViewNodeIDMapping.put(k.getIndex(), v.getIndex()));

        this.planNodeId = queryStructInfo.getTopPlan().getGroupExpression()
                .map(GroupExpression::getId).orElseGet(() -> new ObjectId(-1));

        this.queryToViewFilterEdgeExpressionMappingList = ExpressionMapping.EMPTY_INSTANCE
                .generateExpressionMappingCombine(viewToQuerySlotMapping,
                        queryStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.FILTER_EDGE),
                        viewStructInfo.getShuttledExpressionsToExpressionsMap().get(ExpressionPosition.FILTER_EDGE));
    }

    public BiMap<StructInfoNode, StructInfoNode> getQueryToViewNodeMapping() {
        return queryToViewNodeMapping;
    }

    public BiMap<Integer, Integer> getQueryToViewNodeIDMapping() {
        return queryToViewNodeIDMapping;
    }

    public Expression getViewJoinExprFromQuery(Expression queryJoinExpr) {
        return queryToViewJoinEdgeExpressionMappingSupplier.get().get(queryJoinExpr);
    }

    public Expression getViewFilterExprFromQuery(Expression queryJoinExpr) {
        return queryToViewFilterEdgeExpressionMappingSupplier.get().get(queryJoinExpr);
    }

    public Expression getViewNodeExprFromQuery(Expression queryJoinExpr) {
        return queryToViewNodeExpressionMappingSupplier.get().get(queryJoinExpr);
    }

    /**
     * Generate logical compatibility context,
     * this make expression mapping between query and view by relation and the slot in relation mapping
     */
    public static LogicalCompatibilityContext from(RelationMapping relationMapping,
            SlotMapping viewToQuerySlotMapping,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo) {
        // init node mapping
        BiMap<StructInfoNode, StructInfoNode> queryToViewNodeMapping = HashBiMap.create();
        Map<RelationId, StructInfoNode> queryRelationIdStructInfoNodeMap
                = queryStructInfo.getRelationIdStructInfoNodeMap();
        Map<RelationId, StructInfoNode> viewRelationIdStructInfoNodeMap
                = viewStructInfo.getRelationIdStructInfoNodeMap();
        for (Map.Entry<MappedRelation, MappedRelation> relationMappingEntry :
                relationMapping.getMappedRelationMap().entrySet()) {
            StructInfoNode queryStructInfoNode = queryRelationIdStructInfoNodeMap.get(
                    relationMappingEntry.getKey().getRelationId());
            StructInfoNode viewStructInfoNode = viewRelationIdStructInfoNodeMap.get(
                    relationMappingEntry.getValue().getRelationId());
            if (queryStructInfoNode != null && viewStructInfoNode != null) {
                queryToViewNodeMapping.put(queryStructInfoNode, viewStructInfoNode);
            }
        }
        return new LogicalCompatibilityContext(queryToViewNodeMapping,
                viewToQuerySlotMapping.toSlotReferenceMap(),
                queryStructInfo,
                viewStructInfo);
    }

    /**
     * Construct LogicalCompatibilityContext with queryToViewFilterEdgeExpressionMapping
     */
    public LogicalCompatibilityContext withQueryToViewFilterEdgeExpressionMapping(
            BiMap<Expression, Expression> queryToViewFilterEdgeExpressionMapping) {
        return new LogicalCompatibilityContext(this.queryToViewNodeMapping,
                this.queryToViewNodeIDMapping,
                this.planNodeId,
                this.queryToViewJoinEdgeExpressionMappingSupplier.get(),
                this.queryToViewNodeExpressionMappingSupplier.get(), queryToViewFilterEdgeExpressionMapping,
                this.queryToViewFilterEdgeExpressionMappingList);
    }

    public List<BiMap<Expression, Expression>> getQueryToViewFilterEdgeExpressionMappingList() {
        return queryToViewFilterEdgeExpressionMappingList;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalCompatibilityContext",
                "queryToViewNodeMapping", queryToViewNodeMapping.toString(),
                "queryToViewJoinEdgeExpressionMapping",
                queryToViewJoinEdgeExpressionMappingSupplier.get() == null
                        ? "" : queryToViewJoinEdgeExpressionMappingSupplier.get().toString(),
                "queryToViewNodeExpressionMapping",
                queryToViewNodeExpressionMappingSupplier.get() == null
                        ? "" : queryToViewNodeExpressionMappingSupplier.get().toString(),
                "queryToViewFilterEdgeExpressionMapping",
                queryToViewFilterEdgeExpressionMappingSupplier.get() == null
                        ? "" : queryToViewFilterEdgeExpressionMappingSupplier.get().toString());
    }
}
