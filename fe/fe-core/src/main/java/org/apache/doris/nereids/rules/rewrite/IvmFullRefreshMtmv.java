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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTableWrapper;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.StatementContext.IvmFallbackStreamScanContext;
import org.apache.doris.nereids.StatementContext.IvmFallbackStreamScanMode;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Replaces base-table OLAP scans in IVM fallback refresh with stream reset/snapshot scans.
 *
 * <p>The rule is enabled only by {@link StatementContext.IvmFallbackStreamScanContext}. That keeps normal
 * COMPLETE/PARTITIONS refresh planning unchanged, while fallback refresh can reuse the existing stream
 * consumption path to advance offsets after the insert transaction commits.
 */
public class IvmFullRefreshMtmv extends DefaultPlanRewriter<Void> implements CustomRewriter {
    private StatementContext statementContext;
    private MTMV mtmv;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        statementContext = jobContext.getCascadesContext().getStatementContext();
        if (!statementContext.hasIvmFallbackStreamScanContexts()) {
            return plan;
        }
        IvmRewriteContext rewriteContext = statementContext.getIvmRewriteContext()
                .orElseThrow(() -> new AnalysisException("IVM fallback stream scan requires rewrite context"));
        if (rewriteContext.getMode() != IvmRewriteContext.Mode.FULL) {
            throw new AnalysisException("IVM fallback stream scan requires FULL rewrite mode");
        }
        mtmv = rewriteContext.getMtmv();
        return plan.accept(this, null);
    }

    @Override
    public Plan visitLogicalOlapTableStreamScan(LogicalOlapTableStreamScan scan, Void context) {
        // The rule may run more than once in the same analyzer pipeline; already rewritten stream scans are stable.
        return scan;
    }

    @Override
    public Plan visitLogicalOlapScan(LogicalOlapScan scan, Void context) {
        OlapTable baseTable = unwrapOlapTable(scan.getTable());
        BaseTableInfo baseTableInfo = new BaseTableInfo(baseTable);
        Optional<IvmFallbackStreamScanContext> scanContext =
                statementContext.getIvmFallbackStreamScanContext(baseTableInfo);
        if (!scanContext.isPresent()) {
            return scan;
        }
        // Replace only the captured base table. Other tables remain ordinary OLAP scans.
        LogicalOlapTableStreamScan streamScan = createStreamScan(scan, baseTable, scanContext.get());
        return projectToOriginSlots(scan, streamScan);
    }

    private LogicalOlapTableStreamScan createStreamScan(LogicalOlapScan scan, OlapTable baseTable,
            IvmFallbackStreamScanContext scanContext) {
        OlapTableStream stream = IvmUtil.getIvmStream(mtmv, baseTable);
        List<Long> selectedPartitionIds = new ArrayList<>(scanContext.getCapturedUpdate().getNext().keySet());
        selectedPartitionIds.sort(Long::compareTo);
        OlapTableStreamWrapper streamWrapper = new OlapTableStreamWrapper(
                stream, baseTable, selectedPartitionIds, scanContext.getCapturedUpdate());
        LogicalOlapTableStreamScan streamScan = new LogicalOlapTableStreamScan(
                StatementScopeIdGenerator.newRelationId(), streamWrapper, scan.getQualifier(),
                selectedPartitionIds, scan.getSelectedTabletIds(), scan.getHints(),
                scan.getTableSample(), scan.getOperativeSlots());
        if (scanContext.getScanMode() == IvmFallbackStreamScanMode.RESET) {
            return streamScan.withIsReset(true);
        }
        return streamScan.withIsSnapshot(true);
    }

    private LogicalProject<?> projectToOriginSlots(LogicalOlapScan oldScan, LogicalOlapTableStreamScan streamScan) {
        Map<String, Slot> streamSlotByName = new HashMap<>();
        for (Slot slot : streamScan.getOutput()) {
            streamSlotByName.put(slot.getName(), slot);
        }

        List<NamedExpression> projects = new ArrayList<>();
        for (Slot oldSlot : oldScan.getOutput()) {
            Slot streamSlot = streamSlotByName.get(oldSlot.getName());
            if (streamSlot != null) {
                // Keep the original expr id so upper expressions bound before this rewrite still resolve.
                projects.add(aliasToOriginSlot(oldSlot, streamSlot));
            } else if (oldSlot.getName().startsWith(Column.HIDDEN_COLUMN_PREFIX)) {
                // Reset/snapshot stream scans expose visible base columns; hidden OLAP columns are filled here.
                projects.add(aliasToOriginSlot(oldSlot, hiddenColumnDefault(oldSlot)));
            } else {
                throw new AnalysisException("IVM full refresh stream scan missing column "
                        + oldSlot.getName() + " for table " + oldScan.getTable().getName());
            }
        }
        return new LogicalProject<>(projects, streamScan);
    }

    private Alias aliasToOriginSlot(Slot originSlot, Expression child) {
        Expression projectedChild = child;
        if (child.nullable() != originSlot.nullable()) {
            projectedChild = originSlot.nullable() ? new Nullable(child) : new NonNullable(child);
        }
        return new Alias(originSlot.getExprId(), ImmutableList.of(projectedChild), originSlot.getName(),
                originSlot.getQualifier(), false);
    }

    private Expression hiddenColumnDefault(Slot slot) {
        if (Column.DELETE_SIGN.equals(slot.getName())) {
            return new TinyIntLiteral((byte) 0);
        }
        if (Column.VERSION_COL.equals(slot.getName()) || Column.COMMIT_TSO_COL.equals(slot.getName())) {
            return new BigIntLiteral(0L);
        }
        return new NullLiteral(slot.getDataType());
    }

    private OlapTable unwrapOlapTable(OlapTable table) {
        OlapTable current = table;
        while (current instanceof OlapTableWrapper) {
            current = ((OlapTableWrapper) current).getOriginTable();
        }
        return current;
    }

}
