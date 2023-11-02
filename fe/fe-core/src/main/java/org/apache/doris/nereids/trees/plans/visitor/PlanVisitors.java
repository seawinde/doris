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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is the facade and factory for common plan visitor.
 */
public class PlanVisitors {

    public static final TableScanCollector TABLE_COLLECTOR_INSTANCE = new TableScanCollector();
    public static final PredicatesCollector PREDICATES_COLLECTOR_INSTANCE = new PredicatesCollector();
    public static final SlotReferenceCollector SLOT_REFERENCE_COLLECTOR = new SlotReferenceCollector();

    /**
     * Collect the table in plan
     * Note: will not get table if table is eliminated by EmptyRelation in rewrite.
     */
    public static class TableScanCollector extends DefaultPlanVisitor<Void, List<CatalogRelation>> {

        @Override
        public Void visit(Plan plan, List<CatalogRelation> collectedRelations) {
            if (plan instanceof CatalogRelation) {
                CatalogRelation catalogRelation = (CatalogRelation) plan;
                collectedRelations.add(catalogRelation);
            }
            return super.visit(plan, collectedRelations);
        }

        @Override
        public Void visitGroupPlan(GroupPlan groupPlan, List<CatalogRelation> context) {
            Group group = groupPlan.getGroup();
            // TODO Should record the struct info on the group?
            return group.getLogicalExpressions().get(0).getPlan().accept(this, context);
        }
    }

    /**
     * Collect the predicates in plan
     */
    public static class PredicatesCollector extends DefaultPlanVisitor<Void, List<Expression>> {

        @Override
        public Void visitLogicalFilter(LogicalFilter<? extends Plan> filter, List<Expression> context) {
            Set<Expression> conjuncts = filter.getConjuncts();
            if (conjuncts == null) {
                return super.visit(filter, context);
            }
            for (Expression conjunct : conjuncts) {
                context.addAll(ExpressionUtils.decomposeAnd(conjunct));
            }
            return super.visit(filter, context);
        }

        @Override
        public Void visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, List<Expression> context) {
            List<Expression> conjuncts = join.getHashJoinConjuncts();
            // TODO Check getOtherJoinConjuncts method predicates
            if (conjuncts == null) {
                return super.visit(join, context);
            }
            for (Expression conjunct : conjuncts) {
                context.addAll(ExpressionUtils.decomposeAnd(conjunct));
            }
            return super.visit(join, context);
        }

        @Override
        public Void visitGroupPlan(GroupPlan groupPlan, List<Expression> context) {
            Group group = groupPlan.getGroup();
            // TODO Should record the struct info on the group?
            return group.getLogicalExpressions().get(0).getPlan().accept(this, context);
        }
    }

    /**
     * SlotReferenceCollector
     */
    public static class SlotReferenceCollector
            extends DefaultPlanVisitor<Void, Set<SlotReference>> {
        @Override
        public Void visit(Plan plan, Set<SlotReference> collectedExpressions) {
            List<? extends Expression> expressions = plan.getExpressions();
            if (expressions.isEmpty()) {
                return super.visit(plan, collectedExpressions);
            }
            expressions.forEach(expression -> {
                if (expression instanceof SlotReference && ((SlotReference) expression).getRelationId()
                        .isPresent()) {
                    collectedExpressions.add((SlotReference) expression);
                }
            });
            return super.visit(plan, collectedExpressions);
        }

        @Override
        public Void visitGroupPlan(GroupPlan groupPlan, Set<SlotReference> context) {
            Group group = groupPlan.getGroup();
            // TODO Should record the struct info on the group?
            return group.getLogicalExpressions().get(0).getPlan().accept(this, context);
        }
    }

    /**
     * GroupPlanRemover
     */
    public static class GroupPlanRemover
            extends DefaultPlanRewriter<Void> {

        @Override
        public Plan visitGroupPlan(GroupPlan groupPlan, Void context) {
            Group group = groupPlan.getGroup();
            return group.getLogicalExpressions().get(0).getPlan();
        }
    }

    /**
     * SlotReferenceReplacer
     */
    public static class SlotReferenceReplacer
            extends DefaultPlanVisitor<Plan, Map<Slot, Slot>> {

        public static final SlotReferenceReplacer INSTANCE = new SlotReferenceReplacer();

        @Override
        public Plan visit(Plan plan, Map<Slot, Slot> mvScanToQueryMapping) {
            List<Slot> slots = plan.getOutput();
            if (slots.isEmpty()) {
                return super.visit(plan, mvScanToQueryMapping);
            }
            slots.forEach(slot -> slot.accept(ExprReplacer.INSTANCE, mvScanToQueryMapping));
            return super.visit(plan, mvScanToQueryMapping);
        }

        @Override
        public Plan visitGroupPlan(GroupPlan groupPlan, Map<Slot, Slot> mvScanToQueryMapping) {
            Group group = groupPlan.getGroup();
            // TODO Should record the struct info on the group?
            return group.getLogicalExpressions().get(0).getPlan().accept(this, mvScanToQueryMapping);
        }

        /**
         * ExprReplacer
         */
        public static class ExprReplacer extends DefaultExpressionVisitor<Void, Map<Slot, Slot>> {
            public static final ExprReplacer INSTANCE = new ExprReplacer();

            @Override
            public Void visitSlotReference(SlotReference slot, Map<Slot, Slot> context) {
                Slot mappedSlot = context.get(slot);
                if (mappedSlot != null) {
                    slot.changeExprId(mappedSlot.getExprId());
                }
                return super.visit(slot, context);
            }
        }
    }
}
