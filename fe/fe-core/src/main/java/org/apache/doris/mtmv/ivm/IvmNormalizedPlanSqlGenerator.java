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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.Subtract;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Serializes the IVM-normalized logical plan into executable SQL.
 *
 * <p>This is an IVM-only POC serializer. It intentionally supports only the
 * plan shapes produced by {@link org.apache.doris.nereids.rules.rewrite.IvmNormalizeMtmv}.
 */
public class IvmNormalizedPlanSqlGenerator extends PlanVisitor<IvmNormalizedPlanSqlGenerator.Relation, Void> {
    private static final String QUERY_ALIAS_PREFIX = "ivm_q";
    private static final String COLUMN_ALIAS_PREFIX = "ivm_c";
    private static final String SCAN_ALIAS_PREFIX = "ivm_scan";

    private int nextQueryId;
    private int nextScanId;

    public String generate(Plan normalizedPlan) {
        Relation relation = normalizedPlan.accept(this, null);
        return selectFinalOutput(normalizedPlan.getOutput(), relation);
    }

    @Override
    public Relation visit(Plan plan, Void context) {
        throw unsupported("plan node", plan.getClass().getSimpleName());
    }

    @Override
    public Relation visitLogicalResultSink(LogicalResultSink<? extends Plan> sink, Void context) {
        return visitLogicalSink(sink, context);
    }

    @Override
    public Relation visitLogicalSink(LogicalSink<? extends Plan> sink, Void context) {
        Relation child = sink.child().accept(this, context);
        return project(sink.getOutputExprs(), child, false);
    }

    @Override
    public Relation visitLogicalOlapScan(LogicalOlapScan scan, Void context) {
        String scanAlias = SCAN_ALIAS_PREFIX + nextScanId++;
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builderWithExpectedSize(scan.getOutput().size());
        for (int i = 0; i < scan.getOutput().size(); i++) {
            Slot slot = scan.getOutput().get(i);
            selectItems.add(new SelectItem(quoteIdentifier(scanAlias) + "." + quoteIdentifier(slot.getName()),
                    generatedColumnAlias(i)));
        }
        return relationFromSelect(selectItems.build(), scan.getOutput(),
                qualifiedTableName(scan) + " " + quoteIdentifier(scanAlias), "");
    }

    @Override
    public Relation visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        return project(project.getProjects(), project.child().accept(this, context), project.isDistinct());
    }

    @Override
    public Relation visitLogicalSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias, Void context) {
        return passThrough(alias.getOutput(), alias.child().accept(this, context), "");
    }

    @Override
    public Relation visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        Relation child = filter.child().accept(this, context);
        ExpressionSqlRenderer renderer = new ExpressionSqlRenderer(child);
        String where = filter.getConjuncts().stream()
                .map(conjunct -> conjunct.accept(renderer, null))
                .collect(Collectors.joining(" AND "));
        return passThrough(filter.getOutput(), child, "WHERE " + where);
    }

    @Override
    public Relation visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        Relation left = join.left().accept(this, context);
        Relation right = join.right().accept(this, context);
        Scope scope = Scope.merge(left.scope, right.scope);
        ExpressionSqlRenderer renderer = new ExpressionSqlRenderer(scope);

        ImmutableList.Builder<String> conjuncts = ImmutableList.builder();
        for (Expression conjunct : join.getHashJoinConjuncts()) {
            conjuncts.add(conjunct.accept(renderer, null));
        }
        for (Expression conjunct : join.getOtherJoinConjuncts()) {
            conjuncts.add(conjunct.accept(renderer, null));
        }

        StringBuilder from = new StringBuilder()
                .append("(").append(left.sql).append(") ").append(quoteIdentifier(left.alias))
                .append(" ").append(joinTypeSql(join.getJoinType()))
                .append(" (").append(right.sql).append(") ").append(quoteIdentifier(right.alias));
        List<String> onConjuncts = conjuncts.build();
        if (!onConjuncts.isEmpty()) {
            from.append(" ON ").append(String.join(" AND ", onConjuncts));
        }

        return passThrough(join.getOutput(), new Relation("", "", scope), from.toString(), "");
    }

    @Override
    public Relation visitLogicalUnion(LogicalUnion union, Void context) {
        if (union.getQualifier() != Qualifier.ALL) {
            throw new AnalysisException("IVM normalized SQL only supports UNION ALL");
        }
        if (!union.getConstantExprsList().isEmpty()) {
            throw new AnalysisException("IVM normalized SQL does not support constant UNION arms");
        }

        ImmutableList.Builder<String> arms = ImmutableList.builderWithExpectedSize(union.children().size());
        for (Plan childPlan : union.children()) {
            Relation child = childPlan.accept(this, context);
            arms.add(selectChildOutput(childPlan.getOutput(), union.getOutput(), child));
        }
        List<String> unionArms = arms.build();
        return relationFromSql("(" + String.join(") UNION ALL (", unionArms) + ")", union.getOutput());
    }

    @Override
    public Relation visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
        Relation child = aggregate.child().accept(this, context);
        ExpressionSqlRenderer renderer = new ExpressionSqlRenderer(child);
        ImmutableList.Builder<SelectItem> selectItems =
                ImmutableList.builderWithExpectedSize(aggregate.getOutputExpressions().size());
        for (int i = 0; i < aggregate.getOutputExpressions().size(); i++) {
            NamedExpression expression = aggregate.getOutputExpressions().get(i);
            selectItems.add(new SelectItem(expressionSql(expression, renderer), generatedColumnAlias(i)));
        }

        String groupBy = "";
        if (!aggregate.getGroupByExpressions().isEmpty()) {
            groupBy = "GROUP BY " + aggregate.getGroupByExpressions().stream()
                    .map(groupExpr -> groupExpr.accept(renderer, null))
                    .collect(Collectors.joining(", "));
        }
        return relationFromSelect(selectItems.build(), aggregate.getOutput(),
                "(" + child.sql + ") " + quoteIdentifier(child.alias), groupBy);
    }

    @Override
    public Relation visitLogicalLimit(LogicalLimit<? extends Plan> limit, Void context) {
        Relation child = limit.child().accept(this, context);
        StringBuilder suffix = new StringBuilder("LIMIT ").append(limit.getLimit());
        if (limit.getOffset() > 0) {
            suffix.append(" OFFSET ").append(limit.getOffset());
        }
        return passThrough(limit.getOutput(), child, suffix.toString());
    }

    private Relation project(List<NamedExpression> projects, Relation child, boolean distinct) {
        ExpressionSqlRenderer renderer = new ExpressionSqlRenderer(child);
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builderWithExpectedSize(projects.size());
        for (int i = 0; i < projects.size(); i++) {
            NamedExpression project = projects.get(i);
            selectItems.add(new SelectItem(expressionSql(project, renderer), generatedColumnAlias(i)));
        }
        return relationFromSelect(selectItems.build(), slots(projects),
                "(" + child.sql + ") " + quoteIdentifier(child.alias), distinct ? "DISTINCT" : "");
    }

    private Relation passThrough(List<Slot> output, Relation child, String suffix) {
        return passThrough(output, child,
                "(" + child.sql + ") " + quoteIdentifier(child.alias), suffix);
    }

    private Relation passThrough(List<Slot> output, Relation child, String from, String suffix) {
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builderWithExpectedSize(output.size());
        for (int i = 0; i < output.size(); i++) {
            Slot slot = output.get(i);
            selectItems.add(new SelectItem(child.scope.sql(slot), generatedColumnAlias(i)));
        }
        return relationFromSelect(selectItems.build(), output, from, suffix);
    }

    private String selectChildOutput(List<Slot> childOutput, List<Slot> unionOutput, Relation child) {
        ImmutableList.Builder<String> selectItems = ImmutableList.builderWithExpectedSize(unionOutput.size());
        for (int i = 0; i < unionOutput.size(); i++) {
            selectItems.add(child.scope.sql(childOutput.get(i)) + " AS " + quoteIdentifier(generatedColumnAlias(i)));
        }
        return "SELECT " + String.join(", ", selectItems.build())
                + " FROM (" + child.sql + ") " + quoteIdentifier(child.alias);
    }

    private String selectFinalOutput(List<Slot> output, Relation relation) {
        ImmutableList.Builder<String> selectItems = ImmutableList.builderWithExpectedSize(output.size());
        for (Slot slot : output) {
            selectItems.add(relation.scope.sql(slot) + " AS " + quoteIdentifier(slot.getName()));
        }
        return "SELECT " + String.join(", ", selectItems.build())
                + " FROM (" + relation.sql + ") " + quoteIdentifier(relation.alias);
    }

    private Relation relationFromSelect(List<SelectItem> selectItems, List<? extends Slot> output, String from,
            String suffix) {
        String body = selectItems.stream()
                .map(SelectItem::toSql)
                .collect(Collectors.joining(", "));
        StringBuilder sql = new StringBuilder("SELECT ").append(body).append(" FROM ").append(from);
        if (!suffix.isEmpty()) {
            if ("DISTINCT".equals(suffix)) {
                sql = new StringBuilder("SELECT DISTINCT ").append(body).append(" FROM ").append(from);
            } else {
                sql.append(" ").append(suffix);
            }
        }
        return relationFromSql(sql.toString(), output);
    }

    private Relation relationFromSql(String sql, List<? extends Slot> output) {
        String alias = QUERY_ALIAS_PREFIX + nextQueryId++;
        Scope scope = Scope.forOutput(alias, output);
        return new Relation(sql, alias, scope);
    }

    private String expressionSql(NamedExpression expression, ExpressionSqlRenderer renderer) {
        if (expression instanceof Alias) {
            return ((Alias) expression).child().accept(renderer, null);
        }
        return expression.accept(renderer, null);
    }

    private List<Slot> slots(List<NamedExpression> expressions) {
        return expressions.stream().map(NamedExpression::toSlot).collect(ImmutableList.toImmutableList());
    }

    private String qualifiedTableName(LogicalOlapScan scan) {
        ImmutableList.Builder<String> parts = ImmutableList.builder();
        parts.addAll(scan.getQualifier());
        parts.add(scan.getTable().getName());
        return parts.build().stream().map(IvmNormalizedPlanSqlGenerator::quoteIdentifier)
                .collect(Collectors.joining("."));
    }

    private String joinTypeSql(JoinType joinType) {
        switch (joinType) {
            case INNER_JOIN:
                return "INNER JOIN";
            case CROSS_JOIN:
                return "CROSS JOIN";
            case LEFT_OUTER_JOIN:
                return "LEFT OUTER JOIN";
            case RIGHT_OUTER_JOIN:
                return "RIGHT OUTER JOIN";
            default:
                throw new AnalysisException("IVM normalized SQL does not support join type: " + joinType);
        }
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }

    private static String generatedColumnAlias(int index) {
        return COLUMN_ALIAS_PREFIX + index;
    }

    private static AnalysisException unsupported(String kind, String name) {
        return new AnalysisException("IVM normalized SQL does not support " + kind + ": " + name);
    }

    static class Relation {
        private final String sql;
        private final String alias;
        private final Scope scope;

        Relation(String sql, String alias, Scope scope) {
            this.sql = sql;
            this.alias = alias;
            this.scope = scope;
        }
    }

    static class Scope {
        private final Map<Slot, String> slotSql = new IdentityHashMap<>();
        private final Map<ExprId, String> exprIdSql = new HashMap<>();

        static Scope forOutput(String alias, List<? extends Slot> output) {
            Scope scope = new Scope();
            for (int i = 0; i < output.size(); i++) {
                Slot slot = output.get(i);
                String sql = quoteIdentifier(alias) + "." + quoteIdentifier(generatedColumnAlias(i));
                scope.slotSql.put(slot, sql);
                scope.exprIdSql.putIfAbsent(slot.getExprId(), sql);
            }
            return scope;
        }

        static Scope merge(Scope left, Scope right) {
            Scope merged = new Scope();
            merged.slotSql.putAll(left.slotSql);
            merged.slotSql.putAll(right.slotSql);
            merged.exprIdSql.putAll(left.exprIdSql);
            merged.exprIdSql.putAll(right.exprIdSql);
            return merged;
        }

        String sql(Slot slot) {
            String sql = slotSql.get(slot);
            if (sql == null) {
                sql = exprIdSql.get(slot.getExprId());
            }
            if (sql == null) {
                throw new AnalysisException("IVM normalized SQL cannot resolve slot: " + slot);
            }
            return sql;
        }
    }

    private static class SelectItem {
        private final String expressionSql;
        private final String alias;

        private SelectItem(String expressionSql, String alias) {
            this.expressionSql = expressionSql;
            this.alias = alias;
        }

        private String toSql() {
            return expressionSql + " AS " + quoteIdentifier(alias);
        }
    }

    private static class ExpressionSqlRenderer extends ExpressionVisitor<String, Void> {
        private final Scope scope;

        ExpressionSqlRenderer(Relation relation) {
            this(relation.scope);
        }

        ExpressionSqlRenderer(Scope scope) {
            this.scope = scope;
        }

        @Override
        public String visit(Expression expr, Void context) {
            throw unsupported("expression", expr.getClass().getSimpleName());
        }

        @Override
        public String visitAlias(Alias alias, Void context) {
            return alias.child().accept(this, context);
        }

        @Override
        public String visitSlot(Slot slot, Void context) {
            return scope.sql(slot);
        }

        @Override
        public String visitLiteral(Literal literal, Void context) {
            return literal.toSql();
        }

        @Override
        public String visitCast(Cast cast, Void context) {
            return "CAST(" + cast.child().accept(this, context) + " AS " + cast.getDataType().toSql() + ")";
        }

        @Override
        public String visitBinaryOperator(BinaryOperator binaryOperator, Void context) {
            return "(" + binaryOperator.left().accept(this, context)
                    + " " + binaryOperatorSymbol(binaryOperator) + " "
                    + binaryOperator.right().accept(this, context) + ")";
        }

        @Override
        public String visitCompoundPredicate(CompoundPredicate compoundPredicate, Void context) {
            String operator = compoundPredicate.getClass().getSimpleName().toUpperCase();
            return compoundPredicate.children().stream()
                    .map(child -> "(" + child.accept(this, context) + ")")
                    .collect(Collectors.joining(" " + operator + " "));
        }

        @Override
        public String visitNot(Not not, Void context) {
            return "(NOT " + not.child().accept(this, context) + ")";
        }

        @Override
        public String visitIsNull(IsNull isNull, Void context) {
            return isNull.child().accept(this, context) + " IS NULL";
        }

        @Override
        public String visitCaseWhen(CaseWhen caseWhen, Void context) {
            StringBuilder sql = new StringBuilder("CASE");
            caseWhen.getValue().ifPresent(value -> sql.append(" ").append(value.accept(this, context)));
            for (WhenClause whenClause : caseWhen.getWhenClauses()) {
                sql.append(whenClause.accept(this, context));
            }
            caseWhen.getDefaultValue().ifPresent(defaultValue ->
                    sql.append(" ELSE ").append(defaultValue.accept(this, context)));
            return sql.append(" END").toString();
        }

        @Override
        public String visitWhenClause(WhenClause whenClause, Void context) {
            return " WHEN " + whenClause.getOperand().accept(this, context)
                    + " THEN " + whenClause.getResult().accept(this, context);
        }

        @Override
        public String visitBoundFunction(BoundFunction boundFunction, Void context) {
            if (boundFunction instanceof Count && ((Count) boundFunction).isStar()) {
                return "COUNT(*)";
            }
            return boundFunction.getName() + "(" + boundFunction.children().stream()
                    .map(child -> child.accept(this, context))
                    .collect(Collectors.joining(", ")) + ")";
        }

        private String binaryOperatorSymbol(BinaryOperator binaryOperator) {
            if (binaryOperator instanceof Add) {
                return "+";
            } else if (binaryOperator instanceof Subtract) {
                return "-";
            } else if (binaryOperator instanceof Multiply) {
                return "*";
            } else if (binaryOperator instanceof Divide) {
                return "/";
            } else if (binaryOperator instanceof Mod) {
                return "%";
            } else if (binaryOperator instanceof EqualTo) {
                return "=";
            } else if (binaryOperator instanceof NullSafeEqual) {
                return "<=>";
            } else if (binaryOperator instanceof GreaterThan) {
                return ">";
            } else if (binaryOperator instanceof GreaterThanEqual) {
                return ">=";
            } else if (binaryOperator instanceof LessThan) {
                return "<";
            } else if (binaryOperator instanceof LessThanEqual) {
                return "<=";
            }
            throw unsupported("binary expression", binaryOperator.getClass().getSimpleName());
        }
    }
}
