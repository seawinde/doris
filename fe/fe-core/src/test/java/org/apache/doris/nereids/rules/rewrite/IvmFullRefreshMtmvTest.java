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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTableWrapper;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.StatementContext.IvmFallbackStreamScanContext;
import org.apache.doris.nereids.StatementContext.IvmFallbackStreamScanMode;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.StreamConsumptionInfoExtractor;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IvmFullRefreshMtmvTest extends TestWithFeService {
    private static final String DB_NAME = "test_ivm_full_refresh_mtmv";
    private static final String BASE_TABLE_NAME = "base_tbl";
    private static final long MV_ID = 10L;

    @Override
    public void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;
        Config.enable_feature_binlog = true;

        createDatabaseAndUse(DB_NAME);
        createTable("CREATE TABLE " + BASE_TABLE_NAME + " (k1 int, v1 int)\n"
                + "UNIQUE KEY(k1)\n"
                + "PARTITION BY RANGE(k1)\n"
                + "(PARTITION p1 VALUES LESS THAN (\"100\"),\n"
                + " PARTITION p2 VALUES LESS THAN (\"200\"))\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true',"
                + " 'binlog.enable' = 'true', 'binlog.need_historical_value' = 'true',"
                + " 'binlog.format' = 'ROW')");
        createTable("CREATE STREAM `" + streamName(baseTable())
                + "` ON TABLE " + BASE_TABLE_NAME);
        createTable("CREATE TABLE wrong_base_tbl (k1 int, v1 int)\n"
                + "UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true')");
    }

    @Test
    public void testReplaceWrappedOlapScanWithResetStreamScanAndKeepExprIds() throws Exception {
        OlapTable baseTable = baseTable();
        Long partitionId = baseTable.getPartition("p1").getId();
        Map<Long, Pair<Long, Long>> tsoRanges = ImmutableMap.of(partitionId, Pair.of(null, 100L));
        OlapTableWrapper wrappedTable = new OlapTableWrapper(baseTable, tsoRanges);
        Assertions.assertTrue(wrappedTable.getEnableUniqueKeyMergeOnWrite());
        Assertions.assertTrue(wrappedTable.getBinlogConfig().isEnableForStreaming());
        LogicalOlapScan scan = newOlapScan(wrappedTable, Lists.newArrayList(partitionId));

        Plan result = rewrite(scan, baseTable, IvmFallbackStreamScanMode.RESET, Lists.newArrayList(partitionId));

        Assertions.assertTrue(result instanceof LogicalProject);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertTrue(project.child() instanceof LogicalOlapTableStreamScan);
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) project.child();
        Assertions.assertTrue(streamScan.isReset());
        Assertions.assertFalse(streamScan.isSnapshot());
        Assertions.assertSame(baseTable, streamScan.getTable().getBaseTable());
        Assertions.assertEquals(Lists.newArrayList(partitionId), streamScan.getSelectedPartitionIds());
        Assertions.assertEquals(100L, streamScan.getTable().getOutputUpdate().getNext().get(partitionId));
        List<TableStreamUpdateInfo> streamUpdates = StreamConsumptionInfoExtractor.extract(result);
        Assertions.assertEquals(1, streamUpdates.size());
        OlapTableStreamUpdate commitUpdate = (OlapTableStreamUpdate) streamUpdates.get(0).getUpdate();
        Assertions.assertEquals(100L, commitUpdate.getNext().get(partitionId));
        assertProjectKeepsOriginSlots(scan, project);
        assertHiddenColumnDefault(project, Column.DELETE_SIGN, TinyIntLiteral.class, (byte) 0);
        assertHiddenColumnDefault(project, Column.VERSION_COL, BigIntLiteral.class, 0L);
    }

    @Test
    public void testReplaceOlapScanWithSnapshotStreamScan() throws Exception {
        OlapTable baseTable = baseTable();
        List<Long> partitionIds = baseTable.getPartitionIds();
        LogicalOlapScan scan = newOlapScan(baseTable, partitionIds);

        Plan result = rewrite(scan, baseTable, IvmFallbackStreamScanMode.SNAPSHOT, partitionIds);

        Assertions.assertTrue(result instanceof LogicalProject);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertTrue(project.child() instanceof LogicalOlapTableStreamScan);
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) project.child();
        Assertions.assertTrue(streamScan.isSnapshot());
        Assertions.assertFalse(streamScan.isReset());
        Assertions.assertEquals(partitionIds.stream().sorted().collect(java.util.stream.Collectors.toList()),
                streamScan.getSelectedPartitionIds());
        assertProjectKeepsOriginSlots(scan, project);
    }

    @Test
    public void testStreamScanCopyBuildersKeepStreamType() throws Exception {
        OlapTable baseTable = baseTable();
        Long partitionId = baseTable.getPartition("p1").getId();
        LogicalOlapScan scan = newOlapScan(baseTable, Lists.newArrayList(partitionId));

        Plan result = rewrite(scan, baseTable, IvmFallbackStreamScanMode.RESET, Lists.newArrayList(partitionId));
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) ((LogicalProject<?>) result).child();
        LogicalOlapScan parentTypedScan = streamScan;

        assertCopiedResetStreamScan(parentTypedScan.withTableAlias("stream_alias"));
        assertCopiedResetStreamScan(parentTypedScan.withMaterializedIndexSelected(baseTable.getBaseIndexId()));
        assertCopiedResetStreamScan(parentTypedScan.withColToSubPathsMap(ImmutableMap.of()));
        assertCopiedResetStreamScan(parentTypedScan.withVirtualColumns(ImmutableList.of()));
        assertCopiedResetStreamScan(parentTypedScan.appendVirtualColumns(ImmutableList.of()));
        assertCopiedResetStreamScan(parentTypedScan.appendVirtualColumnsAndTopN(ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), ImmutableList.of(), Optional.empty(), Optional.empty()));
        LogicalOlapScan partitionPrunedScan = parentTypedScan.withSelectedPartitionIds(Lists.newArrayList(partitionId));
        assertCopiedResetStreamScan(partitionPrunedScan);
        Assertions.assertTrue(partitionPrunedScan.isPartitionPruned());
        Assertions.assertFalse(partitionPrunedScan.hasPartitionPredicate());
        LogicalOlapScan partitionPredicateScan = parentTypedScan.withSelectedPartitionIds(
                Lists.newArrayList(partitionId), true);
        assertCopiedResetStreamScan(partitionPredicateScan);
        Assertions.assertTrue(partitionPredicateScan.isPartitionPruned());
        Assertions.assertTrue(partitionPredicateScan.hasPartitionPredicate());
        assertCopiedResetStreamScan((LogicalOlapScan) parentTypedScan.withGroupExprLogicalPropChildren(
                Optional.empty(), Optional.empty(), ImmutableList.of()));

        Plan snapshotResult = rewrite(scan, baseTable, IvmFallbackStreamScanMode.SNAPSHOT,
                Lists.newArrayList(partitionId));
        LogicalOlapScan snapshotScan = (LogicalOlapTableStreamScan) ((LogicalProject<?>) snapshotResult).child();
        assertCopiedSnapshotStreamScan(snapshotScan.withTableAlias("snapshot_alias"));
        assertCopiedSnapshotStreamScan(snapshotScan.withMaterializedIndexSelected(baseTable.getBaseIndexId()));
        assertCopiedSnapshotStreamScan(snapshotScan.withColToSubPathsMap(ImmutableMap.of()));
        assertCopiedSnapshotStreamScan(snapshotScan.withVirtualColumns(ImmutableList.of()));
        assertCopiedSnapshotStreamScan(snapshotScan.appendVirtualColumns(ImmutableList.of()));
        assertCopiedSnapshotStreamScan(snapshotScan.appendVirtualColumnsAndTopN(ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), ImmutableList.of(), Optional.empty(), Optional.empty()));
        assertCopiedSnapshotStreamScan(snapshotScan.withSelectedPartitionIds(Lists.newArrayList(partitionId)));
        assertCopiedSnapshotStreamScan((LogicalOlapScan) snapshotScan.withGroupExprLogicalPropChildren(
                Optional.empty(), Optional.empty(), ImmutableList.of()));
    }

    @Test
    public void testRewriteIsIdempotentInSameCascadesContext() throws Exception {
        OlapTable baseTable = baseTable();
        Long partitionId = baseTable.getPartition("p1").getId();
        LogicalOlapScan scan = newOlapScan(baseTable, Lists.newArrayList(partitionId));
        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase(DB_NAME);
        StatementContext statementContext = new StatementContext(ctx, null);
        BaseTableInfo baseTableInfo = new BaseTableInfo(baseTable);
        statementContext.setIvmFallbackStreamScanContexts(
                ImmutableMap.of(baseTableInfo, new IvmFallbackStreamScanContext(
                        IvmFallbackStreamScanMode.RESET, capturedUpdate(Lists.newArrayList(partitionId)))));
        statementContext.setIvmRewriteContext(Optional.of(IvmRewriteContext.full(mtmv())));
        CascadesContext cascadesContext = CascadesContext.initContext(statementContext, scan, PhysicalProperties.ANY);
        JobContext jobContext = new JobContext(cascadesContext, PhysicalProperties.ANY);
        IvmFullRefreshMtmv rewriter = new IvmFullRefreshMtmv();

        Plan result = rewriter.rewriteRoot(scan, jobContext);
        Plan secondResult = new IvmFullRefreshMtmv().rewriteRoot(result, jobContext);

        Assertions.assertTrue(result instanceof LogicalProject);
        Assertions.assertTrue(secondResult instanceof LogicalProject);
        Assertions.assertTrue(((LogicalProject<?>) secondResult).child() instanceof LogicalOlapTableStreamScan);
        Assertions.assertEquals(1, secondResult.collectToList(LogicalOlapTableStreamScan.class::isInstance).size());
    }

    @Test
    public void testIvmStreamValidationFailsClosed() throws Exception {
        OlapTable baseTable = baseTable();
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTable wrongBaseTable = (OlapTable) db.getTableOrMetaException("wrong_base_tbl");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException(streamName(baseTable));

        Assertions.assertThrows(IvmException.class,
                () -> IvmUtil.getIvmStream(mtmv(MV_ID + 1, "missing_mv"), baseTable));
        Assertions.assertThrows(IvmException.class,
                () -> IvmUtil.getIvmStream(mtmv(), wrongBaseTable));

        stream.setDisabled(true);
        try {
            Assertions.assertThrows(IvmException.class,
                    () -> IvmUtil.getIvmStream(mtmv(), baseTable));
        } finally {
            stream.setDisabled(false);
        }
        stream.setStale(true);
        try {
            Assertions.assertThrows(IvmException.class,
                    () -> IvmUtil.getIvmStream(mtmv(), baseTable));
        } finally {
            stream.setStale(false);
        }
    }

    @Test
    public void testResolveStreamAfterMtmvRename() throws Exception {
        OlapTable baseTable = baseTable();
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException(streamName(baseTable));

        Assertions.assertSame(stream, IvmUtil.getIvmStream(mtmv(MV_ID, "renamed_mv"), baseTable));
    }

    private Plan rewrite(LogicalOlapScan scan, OlapTable baseTable, IvmFallbackStreamScanMode mode,
            List<Long> selectedPartitionIds) throws Exception {
        ConnectContext ctx = createDefaultCtx();
        ctx.setDatabase(DB_NAME);
        StatementContext statementContext = new StatementContext(ctx, null);
        BaseTableInfo baseTableInfo = new BaseTableInfo(baseTable);
        statementContext.setIvmFallbackStreamScanContexts(
                ImmutableMap.of(baseTableInfo, new IvmFallbackStreamScanContext(
                        mode, capturedUpdate(selectedPartitionIds))));
        statementContext.setIvmRewriteContext(Optional.of(IvmRewriteContext.full(mtmv())));
        CascadesContext cascadesContext = CascadesContext.initContext(statementContext, scan, PhysicalProperties.ANY);
        return new IvmFullRefreshMtmv().rewriteRoot(scan,
                new JobContext(cascadesContext, PhysicalProperties.ANY));
    }

    private LogicalOlapScan newOlapScan(OlapTable table, List<Long> partitionIds) throws Exception {
        StatementScopeIdGenerator.clear();
        return new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), table,
                ImmutableList.of(DB_NAME, table.getName()), partitionIds, ImmutableList.of(),
                ImmutableList.of(), Optional.empty(), ImmutableList.of());
    }

    private void assertProjectKeepsOriginSlots(LogicalOlapScan scan, LogicalProject<?> project) {
        Assertions.assertEquals(scan.getOutput().size(), project.getOutput().size());
        for (int i = 0; i < scan.getOutput().size(); i++) {
            Assertions.assertEquals(scan.getOutput().get(i).getName(), project.getOutput().get(i).getName());
            Assertions.assertEquals(scan.getOutput().get(i).getExprId(), project.getOutput().get(i).getExprId());
            Assertions.assertEquals(scan.getOutput().get(i).getQualifier(), project.getOutput().get(i).getQualifier());
            Assertions.assertEquals(scan.getOutput().get(i).nullable(), project.getOutput().get(i).nullable(),
                    "nullable mismatch for " + scan.getOutput().get(i).getName());
        }
    }

    private void assertCopiedResetStreamScan(LogicalOlapScan scan) {
        Assertions.assertTrue(scan instanceof LogicalOlapTableStreamScan);
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) scan;
        Assertions.assertTrue(streamScan.isReset());
        Assertions.assertFalse(streamScan.isSnapshot());
    }

    private void assertCopiedSnapshotStreamScan(LogicalOlapScan scan) {
        Assertions.assertTrue(scan instanceof LogicalOlapTableStreamScan);
        LogicalOlapTableStreamScan streamScan = (LogicalOlapTableStreamScan) scan;
        Assertions.assertFalse(streamScan.isReset());
        Assertions.assertTrue(streamScan.isSnapshot());
    }

    private OlapTableStreamUpdate capturedUpdate(List<Long> partitionIds) {
        Map<Long, Long> next = new HashMap<>();
        for (int i = 0; i < partitionIds.size(); i++) {
            next.put(partitionIds.get(i), 100L + i);
        }
        return new OlapTableStreamUpdate(new HashMap<>(), next);
    }

    private String streamName(OlapTable baseTable) {
        return IvmUtil.streamName(mtmv(), baseTable);
    }

    private MTMV mtmv() {
        return mtmv(MV_ID, "test_mv");
    }

    private MTMV mtmv(long id, String name) {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getId()).thenReturn(id);
        Mockito.when(mtmv.getName()).thenReturn(name);
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn(DB_NAME);
        return mtmv;
    }

    private void assertHiddenColumnDefault(LogicalProject<?> project, String name,
            Class<?> literalClass, Object expectedValue) {
        Optional<NamedExpression> hiddenProject = project.getProjects().stream()
                .filter(expr -> name.equals(expr.getName()))
                .findFirst();
        Assertions.assertTrue(hiddenProject.isPresent());
        Assertions.assertInstanceOf(Alias.class, hiddenProject.get());
        Alias alias = (Alias) hiddenProject.get();
        Assertions.assertInstanceOf(literalClass, alias.child());
        if (alias.child() instanceof TinyIntLiteral) {
            Assertions.assertEquals(expectedValue, ((TinyIntLiteral) alias.child()).getValue());
        } else if (alias.child() instanceof BigIntLiteral) {
            Assertions.assertEquals(expectedValue, ((BigIntLiteral) alias.child()).getValue());
        }
    }

    private OlapTable baseTable() throws Exception {
        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        return (OlapTable) db.getTableOrMetaException(BASE_TABLE_NAME);
    }
}
