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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.joinorder.JoinOrderJob;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.metadata.EquivalenceClass;
import org.apache.doris.nereids.trees.metadata.PlanMetadataQuery;
import org.apache.doris.nereids.trees.metadata.Predicates;
import org.apache.doris.nereids.trees.metadata.Predicates.SplitPredicate;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitors;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * AbstractMaterializedViewRule
 */
public abstract class AbstractMaterializedViewRule {

    protected List<Plan> rewrite(LogicalProject queryTopProject, Plan queryPlan, CascadesContext cascadesContext) {
        List<MaterializationContext> materializationContexts = cascadesContext.getMaterializationContexts();
        List<Plan> rewriteResults = new ArrayList<>();
        if (materializationContexts.isEmpty()) {
            return rewriteResults;
        }
        // check query queryPlan
        if (!isPatternSupport(queryTopProject, queryPlan)) {
            return rewriteResults;
        }
        StructInfo queryStructInfo = extractStructInfo(queryTopProject, queryPlan, cascadesContext);
        if (!checkStructInfo(queryStructInfo)) {
            return rewriteResults;
        }

        // PoC hyper graph query
        // HyperGraph queryHyperGraph = new HyperGraph();
        // Plan plan = queryStructInfo.getPlan();
        // plan = plan.accept(new PlanVisitors.GroupPlanRemover(), null);
        // Group poCGroup = cascadesContext.getMemo().initPoC(plan);
        // JoinOrderJob joinOrderJob = new JoinOrderJob(poCGroup, cascadesContext.getCurrentJobContext());
        // joinOrderJob.buildGraph(poCGroup, queryHyperGraph);

        for (MaterializationContext materializationContext : materializationContexts) {
            Plan mvPlan = materializationContext.getMvPlan();
            LogicalProject viewTopProject;
            Plan viewPlan;
            // TODO get table and scan from materialization context
            // poc child(0) remove the logical result sink
            Plan viewScanNode = materializationContext.getScanPlan().child(0);
            if (mvPlan instanceof Project) {
                viewTopProject = (LogicalProject) mvPlan;
                viewPlan = mvPlan.child(0);
            } else {
                viewTopProject = null;
                viewPlan = mvPlan;
            }
            // TODO Normalize to remove resultSink
            if (viewPlan instanceof LogicalResultSink) {
                viewPlan = (Plan) ((LogicalResultSink) viewPlan).child();
            }
            if (!isPatternSupport(viewTopProject, viewPlan)) {
                continue;
            }
            StructInfo viewStructInfo = extractStructInfo(viewTopProject, viewPlan, cascadesContext);
            if (!checkStructInfo(viewStructInfo)) {
                continue;
            }

            // Poc Hyper graph view
            HyperGraph viewHyperGraph = new HyperGraph();
            Plan view = viewStructInfo.getPlan();
            view = view.accept(new PlanVisitors.GroupPlanRemover(), null);
            Group poCViewGroup = cascadesContext.getMemo().initPoC(view);
            JoinOrderJob viewJoinOrderJob = new JoinOrderJob(poCViewGroup, cascadesContext.getCurrentJobContext());
            viewJoinOrderJob.buildGraph(poCViewGroup, viewHyperGraph);

            MatchMode matchMode = decideMatchMode(queryStructInfo, viewStructInfo);
            if (MatchMode.NOT_MATCH == matchMode) {
                continue;
            }
            List<BiMap<RelationId, RelationId>> queryToViewTableMappings =
                    generateRelationMap(queryStructInfo, viewStructInfo);
            for (BiMap<RelationId, RelationId> queryToViewTableMapping : queryToViewTableMappings) {
                Expression compensatePredicates = predicatesCompensate(queryStructInfo, viewStructInfo,
                        queryToViewTableMapping);
                if (compensatePredicates == null) {
                    continue;
                }
                Plan rewritedPlan;
                if (compensatePredicates instanceof BooleanLiteral
                        && ((BooleanLiteral) compensatePredicates).getValue()) {
                    rewritedPlan = viewScanNode;
                } else {
                    // try to compensate predicates by using mv scan, Poc is always true
                    List<Expression> rewriteCompensatePredicates = rewriteExpression(
                            ImmutableList.of(compensatePredicates),
                            queryStructInfo,
                            viewStructInfo,
                            queryToViewTableMapping.inverse(),
                            viewScanNode);
                    if (rewriteCompensatePredicates.isEmpty()) {
                        continue;
                    }
                    rewritedPlan = new LogicalFilter<>(Sets.newHashSet(rewriteCompensatePredicates), viewScanNode);
                }
                rewritedPlan = rewriteView(matchMode, queryStructInfo, viewStructInfo,
                        queryToViewTableMapping, rewritedPlan);
                if (rewritedPlan == null) {
                    continue;
                }
                rewriteResults.add(rewritedPlan);
            }
        }
        return rewriteResults;
    }

    // Aggregate rewriting(roll up) or join rewriting according to different inherit class implement
    protected Plan rewriteView(MatchMode matchMode,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            BiMap<RelationId, RelationId> queryToViewTableMappings,
            Plan temporaryRewrite) {
        return temporaryRewrite;
    }

    protected SplitPredicate rewriteExpression(SplitPredicate splitPredicate,
            StructInfo sourceStructInfo,
            StructInfo targetStructInfo,
            BiMap<RelationId, RelationId> sourceToTargetMapping,
            Plan targetScanNode) {
        // call below rewriteExpression
        return null;
    }

    // Use target targetScanNode out to represent the source expression
    protected List<Expression> rewriteExpression(List<? extends Expression> sourceExpression,
            StructInfo sourceStructInfo,
            StructInfo targetStructInfo,
            BiMap<RelationId, RelationId> sourceToTargetMapping,
            Plan targetScanNode) {
        // Firstly, rewrite the target plan output slot using query with inverse mapping
        // and record the position(another way, according to unify bottom mapping and up to bottom mapping separately,
        // then represent query using target)
        // then rewrite the sourceExpression to use the viewScanNode with above map
        //     source                           target
        //        project (slot 1, 2)              project(slot 3, 2, 1)
        //          scan(table)                        scan(table)
        //
        // transform to:
        //    project (slot 2, 1)
        //        target
        //
        return ImmutableList.of(BooleanLiteral.of(true));
    }

    protected Expression predicatesCompensate(
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            BiMap<RelationId, RelationId> queryToViewTableMapping
    ) {
        // TODO Predicate compensate should be common and move to util
        return BooleanLiteral.of(true);
        // Equal predicate compensate
        // EquivalenceClass queryEquivalenceClass = queryStructInfo.getEquivalenceClass();
        // EquivalenceClass viewEquivalenceClass = viewStructInfo.getEquivalenceClass();
        // if (queryEquivalenceClass.isEmpty()
        //         && !viewEquivalenceClass.isEmpty()) {
        //     return null;
        // }
        // // TODO Equals compare the exprId, which should be the absolutely table and column.
        // Mapping mapping = queryEquivalenceClass.generateMapping(viewEquivalenceClass);
        // if (mapping == null) {
        //     return null;
        // }
        // List<Set<SlotReference>> queryValues = queryEquivalenceClass.getEquivalenceValues();
        // List<Set<SlotReference>> viewValues = viewEquivalenceClass.getEquivalenceValues();
        // Expression compensatePredicate = BooleanLiteral.of(true);
        // for (int i = 0; i < queryValues.size(); i++) {
        //     List<Integer> targets = mapping.getTargetBy(i);
        //     if (!targets.isEmpty()) {
        //         // Add only predicates that are not there
        //         for (int j : targets) {
        //             Set<SlotReference> difference = new HashSet<>(queryValues.get(i));
        //             difference.removeAll(viewValues.get(j));
        //             for (SlotReference e : difference) {
        //                 Expression equals = new EqualTo(e, viewValues.get(j).iterator().next());
        //                 compensatePredicate = new And(compensatePredicate, equals);
        //             }
        //         }
        //     } else {
        //         // Add all predicates
        //         Iterator<SlotReference> querySlotIterator = queryValues.get(i).iterator();
        //         SlotReference first = querySlotIterator.next();
        //         while (querySlotIterator.hasNext()) {
        //             Expression equals = new EqualTo(first, querySlotIterator.next());
        //             compensatePredicate = new And(compensatePredicate, equals);
        //         }
        //     }
        //     // TODO RangePredicates and ResidualPredicates
        // }
        // return compensatePredicate;
    }

    // Generate table mapping between query table to view table
    protected List<BiMap<RelationId, RelationId>> generateRelationMap(
            StructInfo queryStructInfo,
            StructInfo viewStrutInfo) {
        List<CatalogRelation> queryRelations = new ArrayList<>();
        PlanVisitors.TABLE_COLLECTOR_INSTANCE.visit(queryStructInfo.getPlan(), queryRelations);

        List<CatalogRelation> viewRelations = new ArrayList<>();
        PlanVisitors.TABLE_COLLECTOR_INSTANCE.visit(viewStrutInfo.getPlan(), viewRelations);

        Multimap<TableIf, RelationId> queryTableRelationIdMap = ArrayListMultimap.create();
        for (CatalogRelation relation : queryRelations) {
            queryTableRelationIdMap.put(relation.getTable(), relation.getRelationId());
        }
        Multimap<TableIf, RelationId> viewTableRelationIdMap = ArrayListMultimap.create();
        for (CatalogRelation relation : viewRelations) {
            viewTableRelationIdMap.put(relation.getTable(), relation.getRelationId());
        }
        // TODO Just support 1:1
        BiMap<RelationId, RelationId> mappingMap = HashBiMap.create();
        for (Map.Entry<TableIf, RelationId> queryEntry : queryTableRelationIdMap.entries()) {
            Collection<RelationId> viewTableIds = viewTableRelationIdMap.get(queryEntry.getKey());
            mappingMap.put(queryEntry.getValue(), viewTableIds.iterator().next());
        }
        return ImmutableList.of(mappingMap);
    }

    protected MatchMode decideMatchMode(StructInfo queryStructInfo, StructInfo viewStructInfo) {
        List<TableIf> queryTableRefs = queryStructInfo.getRelations()
                .stream()
                .map(CatalogRelation::getTable).collect(
                        Collectors.toList());
        List<TableIf> viewTableRefs = viewStructInfo.getRelations()
                .stream()
                .map(CatalogRelation::getTable).collect(
                        Collectors.toList());
        boolean sizeSame = viewTableRefs.size() == queryTableRefs.size();
        boolean queryPartial = viewTableRefs.containsAll(queryTableRefs);
        boolean viewPartial = queryTableRefs.containsAll(viewTableRefs);
        if (sizeSame && queryPartial && viewPartial) {
            return MatchMode.COMPLETE;
        }
        if (!sizeSame && queryPartial) {
            return MatchMode.QUERY_PARTIAL;
        }
        if (!sizeSame && viewPartial) {
            return MatchMode.VIEW_PARTIAL;
        }
        return MatchMode.NOT_MATCH;
    }

    protected boolean checkStructInfo(StructInfo info) {
        if (info.getRelations().isEmpty()) {
            return false;
        }
        if (!info.getPredicates().getCanNotPulledUpPredicates().isEmpty()) {
            return false;
        }
        return true;
    }

    protected StructInfo extractStructInfo(LogicalProject topProject, Plan topInput, CascadesContext cascadesContext) {

        Plan query = topProject == null ? topInput : topProject;
        if (query.getGroupExpression().isPresent()
                && query.getGroupExpression().get().getOwnerGroup().getStructInfo().isPresent()) {
            Group belongGroup = query.getGroupExpression().get().getOwnerGroup();
            return belongGroup.getStructInfo().get();
        } else {
            // PoC build hyper graph and set into group
            HyperGraph hyperGraph = new HyperGraph();
            query = query.accept(new PlanVisitors.GroupPlanRemover(), null);
            Group poCGroup = cascadesContext.getMemo().initPoC(query);
            JoinOrderJob joinOrderJob = new JoinOrderJob(poCGroup, cascadesContext.getCurrentJobContext());
            joinOrderJob.buildGraph(poCGroup, hyperGraph);

            // Support rewrite TableType.OLAP currently
            // get tables
            List<CatalogRelation> relations =
                    PlanMetadataQuery.getTables(topInput, Sets.newHashSet(TableType.OLAP));

            Predicates predicates = PlanMetadataQuery.getPredicates(topInput);

            // get predicates (include join condition and filter)
            Expression composedExpressions = predicates.composedExpression();
            SplitPredicate splitPredicate = Predicates.splitPredicates(composedExpressions);

            // construct equivalenceClass according to equals predicates
            final EquivalenceClass equivalenceClass = new EquivalenceClass();
            List<Expression> equalPredicates =
                    ExpressionUtils.extractConjunction(splitPredicate.getEqualPredicates());
            for (Expression expression : equalPredicates) {
                EqualTo equalTo = (EqualTo) expression;
                equivalenceClass.addEquivalenceClass(
                        (SlotReference) equalTo.getArguments().get(0),
                        (SlotReference) equalTo.getArguments().get(1));
            }

            // set on current group and mv scan not set
            StructInfo structInfo = StructInfo.of(relations, predicates, equivalenceClass,
                    topProject, topInput, hyperGraph);
            if (query.getGroupExpression().isPresent()) {
                query.getGroupExpression().get().getOwnerGroup().setStructInfo(structInfo);
            }
            return structInfo;
        }
    }

    protected boolean isPatternSupport(LogicalProject topProject, Plan plan) {
        return false;
    }

    /**
     * MatchMode
     */
    protected enum MatchMode {
        COMPLETE,
        VIEW_PARTIAL,
        QUERY_PARTIAL,
        NOT_MATCH
    }
}
