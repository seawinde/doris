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

package org.apache.doris.nereids.rules.rewrite.mv;

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitors;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitors.SlotReferenceReplacer.ExprReplacer;

import com.google.common.collect.BiMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AbstractMaterializedViewJoinRule
 */
public abstract class AbstractMaterializedViewJoinRule extends AbstractMaterializedViewRule {

    @Override
    protected Plan rewriteView(MatchMode matchMode,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            BiMap<RelationId, RelationId> queryToViewTableMappings,
            Plan tempRewritedPlan) {

        List<Expression> expressions = rewriteExpression(queryStructInfo.getTopExpressions(),
                queryStructInfo,
                viewStructInfo,
                queryToViewTableMappings,
                tempRewritedPlan
        );
        if (expressions == null) {
            return queryStructInfo.getPlan();
        }
        // PoC Generate mapping from query slot reference to mv slot reference, note: clone
        // if any slot can not map then bail out
        // simplfy implement
        Set<SlotReference> querySlotSet = new HashSet<>();
        queryStructInfo.getPlan().accept(PlanVisitors.SLOT_REFERENCE_COLLECTOR, querySlotSet);

        Set<SlotReference> viewSlotSet = new HashSet<>();
        viewStructInfo.getPlan().accept(PlanVisitors.SLOT_REFERENCE_COLLECTOR, viewSlotSet);

        Map<SlotReference, SlotReference> queryToViewSlotMapping = new HashMap<>();
        for (SlotReference querySlot : querySlotSet) {
            for (SlotReference viewSlot : viewSlotSet) {
                if (Objects.equals(querySlot.getName(), viewSlot.getName())
                        && Objects.equals(querySlot.getQualifier(), viewSlot.getQualifier())) {
                    queryToViewSlotMapping.put(querySlot, viewSlot);
                }
            }
        }
        // PoC Generate mapping from mv sql output to mv scan out put
        Map<SlotReference, SlotReference> mvToMvScanMapping = new HashMap<>();
        List<Slot> mvScanSlotList = tempRewritedPlan.getOutput();
        List<Slot> mvSlotList = viewStructInfo.getPlan().getOutput();
        for (int i = 0; i < mvSlotList.size(); i++) {
            mvToMvScanMapping.put((SlotReference) mvSlotList.get(i), (SlotReference) mvScanSlotList.get(i));
        }

        // TODO check if the query expr can derive from the view
        // PoC If the query expression can get from mv sql, so replace the mv scan slot reference
        // PoC according to the mapping above. Simplify implement
        Map<Slot, Slot> mvScanToQueryMapping = new HashMap<>();
        List<Slot> output = queryStructInfo.getPlan().getOutput();
        for (Slot querySlot : output) {
            Slot mvSlot = queryToViewSlotMapping.get(querySlot);
            if (mvSlot == null) {
                return null;
            }
            SlotReference mvScanSlot = mvToMvScanMapping.get(mvSlot);
            if (mvScanSlot == null) {
                return null;
            }
            mvScanToQueryMapping.put(mvScanSlot, querySlot);
        }
        // Replace the mv scan output with query slot, lazy before add filter and other project

        // tempRewritedPlan.accept(SlotReferenceReplacer.INSTANCE, mvScanToQueryMapping);

        tempRewritedPlan.getOutput().stream()
                .forEach(slot -> slot.accept(ExprReplacer.INSTANCE, mvScanToQueryMapping));
        LogicalProject<Plan> planLogicalProject = new LogicalProject<>(
                output.stream().map(NamedExpression.class::cast).collect(Collectors.toList()),
                tempRewritedPlan);
        return planLogicalProject;
    }

    protected boolean isPatternSupport(LogicalProject topProject, Plan plan) {
        if (topProject != null) {
            return PatternChecker.INSTANCE.visit(topProject, null);
        }
        return PatternChecker.INSTANCE.visit(plan, null);
    }

    static class PatternChecker extends DefaultPlanVisitor<Boolean, Void> {
        public static final PatternChecker INSTANCE = new PatternChecker();

        @Override
        public Boolean visit(Plan plan, Void context) {
            if (plan == null) {
                return true;
            }
            if (!(plan instanceof LogicalPlan)) {
                return false;
            }
            if (!(plan instanceof LogicalRelation)
                    && !(plan instanceof LogicalProject)
                    && !(plan instanceof LogicalFilter)
                    && !(plan instanceof LogicalJoin)) {
                return false;
            }
            super.visit(plan, context);
            return true;
        }

        @Override
        public Boolean visitGroupPlan(GroupPlan groupPlan, Void context) {
            Group group = groupPlan.getGroup();
            return group.getLogicalExpressions().stream()
                    .anyMatch(groupExpression -> groupExpression.getPlan().accept(this, context));
        }
    }
}
