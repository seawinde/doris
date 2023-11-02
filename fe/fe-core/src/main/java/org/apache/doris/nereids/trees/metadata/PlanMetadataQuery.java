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

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitors;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * PlanMetadataQuery
 */
public class PlanMetadataQuery {

    private PlanMetadataQuery() {
    }

    // Replace the slot in expression with the lineage identifier from specified
    // baseTable sets or target table types
    // Note: Maybe unnecessary
    public static Expression shuttleExpressionWithLineage(Plan plan, Expression expression,
            Set<TableType> targetTypes,
            Set<String> tableIdentifiers) {
        return null;
    }

    public static List<CatalogRelation> getTables(Plan plan, Set<TableType> targetTypes) {
        List<CatalogRelation> relations = new ArrayList<>();
        PlanVisitors.TABLE_COLLECTOR_INSTANCE.visit(plan, relations);
        return relations;
    }

    public static Predicates getPredicates(Plan plan) {
        List<Expression> expressions = new ArrayList<>();
        plan.accept(PlanVisitors.PREDICATES_COLLECTOR_INSTANCE, expressions);
        return Predicates.of(new HashSet<>(expressions), ImmutableSet.of());
    }
}
