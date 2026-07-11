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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class IvmRefreshManagerTest {
    private boolean originalEnableTableStream;

    @BeforeEach
    public void enableTableStream() {
        originalEnableTableStream = Config.enable_table_stream;
        Config.enable_table_stream = true;
    }

    @AfterEach
    public void restoreTableStreamConfig() {
        Config.enable_table_stream = originalEnableTableStream;
    }

    @Test
    public void testRefreshContextRejectsNulls() {
        MTMV mtmv = mockMtmv();
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(null, new ConnectContext()));
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmRefreshContext(mtmv, null));
    }

    @Test
    public void testManagerReturnsSuccessForEmptyBundles() {
        MTMV mtmv = mockMtmv();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), Collections.emptyList());
        IvmRefreshResult result = manager.doRefresh(mtmv);
        Assertions.assertTrue(result.isSuccess());
        Assertions.assertFalse(manager.executeCalled);
    }

    @Test
    public void testManagerExecutesBundles() {
        MTMV mtmv = mockMtmv();
        Command cmd = Mockito.mock(Command.class);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), makeCommands(cmd));
        IvmRefreshResult result = manager.doRefresh(mtmv);
        Assertions.assertTrue(result.isSuccess());
        Assertions.assertTrue(manager.executeCalled);
    }

    @Test
    public void testBuildInsertCommandUsesInsertedColumnNamesForIvmSink() {
        MTMV mtmv = mockMtmv();
        List<String> insertedColumns = List.of("k1", Column.IVM_ROW_ID_COL);
        Mockito.when(mtmv.getInsertedColumnNames()).thenReturn(insertedColumns);
        Mockito.when(mtmv.getQuerySql()).thenReturn("select 1 as k1, 2 as " + Column.IVM_ROW_ID_COL);

        IvmRefreshManager manager = new IvmRefreshManager();
        InsertIntoTableCommand command = manager.buildInsertCommand(mtmv);

        Assertions.assertInstanceOf(UnboundTableSink.class, command.getLogicalQuery());
        UnboundTableSink<?> sink = (UnboundTableSink<?>) command.getLogicalQuery();
        Assertions.assertEquals(List.of("k1", Column.IVM_ROW_ID_COL), sink.getColNames());
    }

    @Test
    public void testManagerReturnsFallbackOnExecutorFailure() {
        MTMV mtmv = mockMtmv();
        Command cmd = Mockito.mock(Command.class);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), makeCommands(cmd));
        manager.throwOnExecute = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.INCREMENTAL_EXECUTION_FAILED,
                result.getFailureReason());
        Assertions.assertTrue(manager.executeCalled);
    }

    @Test
    public void testManagerReturnsFallbackWithKnownExecutionFailureReason() {
        assertKnownExecutionFailureFallback(IvmFailureReason.MIN_MAX_BOUNDARY_HIT,
                IvmFailureClassifier.MIN_MAX_BOUNDARY_MSG_PREFIX + ": deleted row may be current MIN value");
        assertKnownExecutionFailureFallback(IvmFailureReason.NON_DETERMINISTIC_ROW_ID,
                IvmFailureClassifier.NON_DETERMINISTIC_ROW_ID_MSG_PREFIX + " in INNER_JOIN");
    }

    @Test
    public void testManagerReturnsBinlogNotEnabledFallbackOnIvmException() {
        MTMV mtmv = mockMtmv();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), Collections.emptyList());
        manager.throwBinlogNotEnabledOnAnalyze = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.BINLOG_NOT_ENABLED, result.getFailureReason());
        Assertions.assertTrue(result.getDetailMessage().contains("no_binlog"));
        Assertions.assertFalse(manager.executeCalled);
    }

    @Test
    public void testManagerReturnsSnapshotFallbackWhenBuildContextFails() {
        MTMV mtmv = mockMtmv();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(null, Collections.emptyList());
        manager.throwOnBuild = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.SNAPSHOT_ALIGNMENT_UNSUPPORTED, result.getFailureReason());
        Assertions.assertFalse(manager.executeCalled);
    }

    @Test
    public void testManagerReturnsIvmExceptionFailureReasonWhenAnalyzeFails() {
        MTMV mtmv = mockMtmv();
        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), Collections.emptyList());
        manager.throwIvmExceptionOnAnalyze = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.AGG_UNSUPPORTED, result.getFailureReason());
        Assertions.assertTrue(result.getDetailMessage().contains("unsupported aggregate"));
        Assertions.assertFalse(manager.executeCalled);
    }

    @Test
    public void testManagerPreservesCurrentPlanSignatureOnMismatch() {
        MTMV mtmv = mockMtmv();
        IvmPlanSignature currentSignature = new IvmPlanSignature("current-layout", "current-sha");
        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), Collections.emptyList());
        manager.planSignatureMismatch = currentSignature;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.PLAN_SIGNATURE_MISMATCH, result.getFailureReason());
        Assertions.assertSame(currentSignature, result.getCurrentPlanSignature());
        Assertions.assertFalse(manager.executeCalled);
    }

    @Test
    public void testManagerReturnsBinlogBrokenBeforeNereidsFlow() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setBinlogBroken(true);
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.BINLOG_BROKEN, result.getFailureReason());
        Assertions.assertFalse(manager.executeCalled);
    }

    @Test
    public void testDebugPointForcesFallbackBeforePrecheck() {
        boolean originalEnableDebugPoints = Config.enable_debug_points;
        try {
            Config.enable_debug_points = true;
            DebugPointUtil.clearDebugPoints();
            DebugPointUtil.addDebugPointWithParams(IvmRefreshManager.DEBUG_POINT_FORCE_FALLBACK_REASON,
                    Collections.singletonMap("reason", IvmFailureReason.PLAN_PATTERN_UNSUPPORTED.name()));

            MTMV mtmv = mockMtmv();
            IvmInfo ivmInfo = new IvmInfo();
            ivmInfo.setBinlogBroken(true);
            Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

            TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), Collections.emptyList());
            manager.useSuperPrecheck = true;
            IvmRefreshResult result = manager.doRefresh(mtmv);

            Assertions.assertFalse(result.isSuccess());
            Assertions.assertEquals(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, result.getFailureReason());
            Assertions.assertFalse(manager.executeCalled);
        } finally {
            DebugPointUtil.clearDebugPoints();
            Config.enable_debug_points = originalEnableDebugPoints;
        }
    }

    @Test
    public void testManagerPrecheckDoesNotConsultExcludedTriggerTables() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        // Empty bundles → success (no-op, all base tables up to date)
        Assertions.assertTrue(result.isSuccess());
        Assertions.assertFalse(manager.executeCalled);
        Mockito.verify(mtmv, Mockito.never()).getExcludedTriggerTables();
    }

    @Test
    public void testManagerPrecheckRejectsDisabledTableStream() {
        MTMV mtmv = mockMtmv();
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        Config.enable_table_stream = false;

        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), Collections.emptyList());
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(IvmFailureReason.STREAM_UNSUPPORTED, result.getFailureReason());
        Assertions.assertFalse(manager.executeCalled);
    }

    @Test
    public void testManagerPassesHealthyPrecheckAndExecutes() {
        MTMV mtmv = mockMtmv();
        Command cmd = Mockito.mock(Command.class);
        IvmInfo ivmInfo = new IvmInfo();
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);

        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), makeCommands(cmd));
        manager.useSuperPrecheck = true;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertTrue(result.isSuccess());
        Assertions.assertTrue(manager.executeCalled);
    }

    private void assertKnownExecutionFailureFallback(IvmFailureReason expectedReason, String detail) {
        MTMV mtmv = mockMtmv();
        Command cmd = Mockito.mock(Command.class);
        TestIvmRefreshManager manager = new TestIvmRefreshManager(newContext(mtmv), makeCommands(cmd));
        manager.failureMessage = detail;

        IvmRefreshResult result = manager.doRefresh(mtmv);

        Assertions.assertFalse(result.isSuccess());
        Assertions.assertEquals(expectedReason, result.getFailureReason());
        Assertions.assertTrue(manager.executeCalled);
    }

    private static IvmRefreshContext newContext(MTMV mtmv) {
        return new IvmRefreshContext(mtmv, new ConnectContext());
    }

    private static MTMV mockMtmv() {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getName()).thenReturn("mv");
        Mockito.when(mtmv.getQualifiedDbName()).thenReturn("db");
        Mockito.when(mtmv.getIvmInfo()).thenReturn(new IvmInfo());
        return mtmv;
    }

    private static List<Command> makeCommands(Command cmd) {
        return Collections.singletonList(cmd);
    }

    private static class TestIvmRefreshManager extends IvmRefreshManager {
        private final IvmRefreshContext context;
        private final List<Command> commands;
        private boolean executeCalled;
        private boolean throwOnExecute;
        private String failureMessage;
        private boolean throwOnBuild;
        private boolean throwIvmExceptionOnAnalyze;
        private boolean throwBinlogNotEnabledOnAnalyze;
        private IvmPlanSignature planSignatureMismatch;
        private boolean useSuperPrecheck;

        private TestIvmRefreshManager(IvmRefreshContext context, List<Command> commands) {
            this.context = context;
            this.commands = commands;
        }

        @Override
        IvmRefreshResult precheck(MTMV mtmv) {
            if (useSuperPrecheck) {
                return super.precheck(mtmv);
            }
            return IvmRefreshResult.success();
        }

        @Override
        IvmRefreshContext buildRefreshContext(MTMV mtmv) throws Exception {
            if (throwOnBuild) {
                throw new AnalysisException("build context failed");
            }
            return context;
        }

        @Override
        void executeInternalRefresh(IvmRefreshContext ctx) throws Exception {
            if (planSignatureMismatch != null) {
                throw IvmException.planSignatureMismatch("layout changed", planSignatureMismatch);
            }
            if (throwIvmExceptionOnAnalyze) {
                throw new IvmException(IvmFailureReason.AGG_UNSUPPORTED, "unsupported aggregate");
            }
            if (throwBinlogNotEnabledOnAnalyze) {
                throw new IvmException(IvmFailureReason.BINLOG_NOT_ENABLED,
                        "binlog is not enabled for table: no_binlog");
            }
            if (commands == null || commands.isEmpty()) {
                return;
            }
            try {
                executeCalled = true;
                if (throwOnExecute || failureMessage != null) {
                    String message = failureMessage != null ? failureMessage : "executor failed";
                    throw new RuntimeException(message);
                }
            } catch (RuntimeException e) {
                throw new IvmException(IvmFailureClassifier.classifyExecutionFailure(e.getMessage())
                        .orElse(IvmFailureReason.INCREMENTAL_EXECUTION_FAILED), e.getMessage());
            }
        }
    }
}
