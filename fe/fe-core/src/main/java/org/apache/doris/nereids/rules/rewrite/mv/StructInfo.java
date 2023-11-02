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

import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.metadata.EquivalenceClass;
import org.apache.doris.nereids.trees.metadata.Predicates;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Project;

import java.util.List;

/**
 * StructInfo
 */
public class StructInfo {
    private final List<CatalogRelation> relations;
    private final Predicates predicates;
    private final EquivalenceClass equivalenceClass;
    private final Project topProject;
    private final Plan topInput;
    private final HyperGraph hyperGraph;

    private StructInfo(List<CatalogRelation> relations,
            Predicates predicates,
            EquivalenceClass equivalenceClass,
            Project topProject,
            Plan topInput,
            HyperGraph hyperGraph) {
        this.relations = relations;
        this.predicates = predicates;
        this.equivalenceClass = equivalenceClass;
        this.topProject = topProject;
        this.topInput = topInput;
        this.hyperGraph = hyperGraph;
    }

    public static StructInfo of(List<CatalogRelation> relations,
            Predicates predicates,
            EquivalenceClass equivalenceClass,
            Project topProject,
            Plan topInput,
            HyperGraph hyperGraph) {
        return new StructInfo(relations, predicates, equivalenceClass, topProject, topInput, hyperGraph);
    }

    public List<CatalogRelation> getRelations() {
        return relations;
    }

    public Predicates getPredicates() {
        return predicates;
    }

    public EquivalenceClass getEquivalenceClass() {
        return equivalenceClass;
    }

    public Plan getTopInput() {
        return topInput;
    }

    public Project getTopProject() {
        return topProject;
    }

    public HyperGraph getHyperGraph() {
        return hyperGraph;
    }

    public List<? extends Expression> getTopExpressions() {
        return getTopProject() == null ? extractReferences(topInput) :
                getTopProject().getProjects();
    }

    public Plan getPlan() {
        return getTopProject() == null ? getTopInput() : (Plan) getTopProject();
    }

    /**
     * It returns a list of references to all columns in the node.
     * If the node is an Aggregate, it returns only the list of references to the grouping columns.
     * The returned list is immutable.
     */
    private List<Expression> extractReferences(Plan plan) {
        return null;
    }
}
