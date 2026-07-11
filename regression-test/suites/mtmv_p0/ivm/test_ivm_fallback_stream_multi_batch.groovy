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

suite("test_ivm_fallback_stream_multi_batch", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def forcedFallbackDebugPoint = "IvmRefreshManager.doRefresh.force_fallback_reason"
    def signatureSaltDebugPoint = "IvmPlanSignatureGenerator.generate.signature_salt"
    def blockAfterCaptureDebugPoint = "MTMVTask.executePartitionBasedRefresh.block_after_capture"
    def failBatchDebugPoint = "MTMVTask.executePartitionBasedRefresh.fail_batch"

    GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)
    GetDebugPoint().disableDebugPointForAllFEs(signatureSaltDebugPoint)
    GetDebugPoint().disableDebugPointForAllFEs(blockAfterCaptureDebugPoint)
    GetDebugPoint().disableDebugPointForAllFEs(failBatchDebugPoint)

    sql """drop materialized view if exists ivm_fbs_mb_mv;"""
    sql """drop table if exists ivm_fbs_o;"""
    sql """drop table if exists ivm_fbs_c;"""

    sql """
        CREATE TABLE ivm_fbs_o (
            order_id INT,
            dt INT,
            cid INT,
            amount INT
        )
        UNIQUE KEY(order_id, dt)
        PARTITION BY RANGE(dt)
        (
            PARTITION p20240101 VALUES LESS THAN ("20240102"),
            PARTITION p20240102 VALUES LESS THAN ("20240103")
        )
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_fbs_c (
            cid INT,
            name VARCHAR(20)
        )
        UNIQUE KEY(cid)
        PARTITION BY RANGE(cid)
        (
            PARTITION p100 VALUES LESS THAN ("100"),
            PARTITION p200 VALUES LESS THAN ("200")
        )
        DISTRIBUTED BY HASH(cid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        );
    """

    def waitVisible = {
        sql "sync"
        sleep(1200)
    }

    sql """
        INSERT INTO ivm_fbs_c VALUES
            (1, 'alice'),
            (101, 'bob');
    """
    sql """
        INSERT INTO ivm_fbs_o VALUES
            (10, 20240101, 1, 10),
            (20, 20240102, 101, 20);
    """
    waitVisible()

    def listIvmStreamNames = {
        sql("""
            SELECT DISTINCT STREAM_NAME
            FROM information_schema.table_stream_consumption
            WHERE DB_NAME = '${context.dbName}'
            ORDER BY STREAM_NAME
        """).collect { row -> row[0].toString() }
                .findAll { streamName -> streamName.startsWith("__doris_ivm_stream_") }
                .toSet()
    }
    def streamNamesBeforeMvCreate = listIvmStreamNames()

    sql """
        CREATE MATERIALIZED VIEW ivm_fbs_mb_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(order_id) BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'refresh_partition_num' = '1'
        )
        AS
        SELECT
            ivm_fbs_o.dt AS dt,
            ivm_fbs_o.order_id AS order_id,
            ivm_fbs_o.cid AS cid,
            ivm_fbs_o.amount AS amount,
            ivm_fbs_c.name AS name
        FROM ivm_fbs_o
        INNER JOIN ivm_fbs_c
            ON ivm_fbs_o.cid = ivm_fbs_c.cid;
    """

    def ivmStreamNames = (listIvmStreamNames() - streamNamesBeforeMvCreate).toSet()
    assertEquals(2, ivmStreamNames.size())

    def queryBaseRows = {
        sql """SET enable_materialized_view_rewrite=false"""
        try {
            sql """
                SELECT
                    CAST(o.dt AS INT),
                    CAST(o.order_id AS INT),
                    CAST(o.cid AS INT),
                    CAST(o.amount AS INT),
                    CAST(c.name AS VARCHAR(20))
                FROM ivm_fbs_o o
                INNER JOIN ivm_fbs_c c
                    ON o.cid = c.cid
                ORDER BY o.dt, o.order_id
            """
        } finally {
            sql """SET enable_materialized_view_rewrite=true"""
        }
    }
    def queryMvRows = {
        sql """
            SELECT dt, order_id, cid, amount, name
            FROM ivm_fbs_mb_mv
            ORDER BY dt, order_id
        """
    }
    def assertMvEqualsBase = {
        assertEquals(queryBaseRows().toString(), queryMvRows().toString())
    }
    def assertLatestRefreshTask = { expectedMode, expectedFallbackReason, expectedPartitionCount ->
        def tasks = sql """
            SELECT RefreshMode, IvmFallbackReason,
                   JSON_LENGTH(NeedRefreshPartitions), JSON_LENGTH(CompletedPartitions), Progress
            FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}'
              AND MvName = 'ivm_fbs_mb_mv'
            ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
        """
        assertEquals(1, tasks.size())
        assertEquals(expectedMode, tasks[0][0].toString())
        def fallbackReason = tasks[0][1] == null ? null : tasks[0][1].toString()
        assertEquals(expectedFallbackReason, fallbackReason == "\\N" ? null : fallbackReason)
        assertEquals(expectedPartitionCount.toString(), tasks[0][2].toString())
        assertEquals(expectedPartitionCount.toString(), tasks[0][3].toString())
        assertEquals("100.00% (${expectedPartitionCount}/${expectedPartitionCount})".toString(),
                tasks[0][4].toString())
    }
    def queryStreamConsumption = {
        def streamNameList = ivmStreamNames.toList().sort()
                .collect { streamName -> "'${streamName}'" }
                .join(", ")
        sql """
            SELECT STREAM_NAME, UNIT, LAG
            FROM information_schema.table_stream_consumption
            WHERE DB_NAME = '${context.dbName}'
              AND STREAM_NAME IN (${streamNameList})
            ORDER BY STREAM_NAME, UNIT
        """
    }
    def assertStreamRows = { consumptionRows ->
        assertEquals(4, consumptionRows.size())
        assertEquals(ivmStreamNames, consumptionRows.collect { row -> row[0].toString() }.toSet())
        assertEquals(["p100", "p200", "p20240101", "p20240102"].toSet(),
                consumptionRows.collect { row -> row[1].toString() }.toSet())
    }
    def assertStreamLagZero = {
        def consumptionRows = queryStreamConsumption()
        assertStreamRows(consumptionRows)
        consumptionRows.each { row -> assertEquals("0", row[2].toString()) }
    }
    def assertOneFactPartitionHasLag = {
        def consumptionRows = queryStreamConsumption()
        assertStreamRows(consumptionRows)
        def factRows = consumptionRows.findAll { row -> row[1].toString().startsWith("p2024") }
        assertEquals(1, factRows.count { row -> row[2].toString() == "0" })
        assertEquals(1, factRows.count { row -> row[2].toString() != "0" })
        consumptionRows.findAll { row -> row[1].toString().startsWith("p1") || row[1].toString() == "p200" }
                .each { row -> assertEquals("0", row[2].toString()) }
    }
    def latestTask = {
        def tasks = sql """
            SELECT TaskId, Status, RefreshMode,
                   JSON_LENGTH(NeedRefreshPartitions), JSON_LENGTH(CompletedPartitions),
                   Progress, LastQueryId, ErrorMsg, IvmFallbackReason
            FROM tasks('type'='mv')
            WHERE MvDatabaseName = '${context.dbName}'
              AND MvName = 'ivm_fbs_mb_mv'
            ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
        """
        tasks.isEmpty() ? null : tasks[0]
    }
    def waitForLatestTask = { predicate, description ->
        long deadline = System.currentTimeMillis() + 60 * 1000
        while (System.currentTimeMillis() < deadline) {
            def task = latestTask()
            if (task != null && predicate(task)) {
                return task
            }
            sleep(200)
        }
        assertTrue(false, "Timed out waiting for ${description}; latest task: ${latestTask()}")
    }

    sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
    assertLatestRefreshTask("COMPLETE", null, 2)
    assertMvEqualsBase()
    assertStreamLagZero()

    // Both streams have zero lag. COMPLETE must still read the non-empty base partitions:
    // the first batch uses RESET and the second batch uses SNAPSHOT for the shared dimension table.
    sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
    assertLatestRefreshTask("COMPLETE", null, 2)
    assertMvEqualsBase()
    assertStreamLagZero()

    sql """INSERT INTO ivm_fbs_o VALUES (11, 20240101, 1, 15);"""
    waitVisible()

    try {
        GetDebugPoint().enableDebugPointForAllFEs(forcedFallbackDebugPoint,
                [reason: "PLAN_PATTERN_UNSUPPORTED"])
        sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv AUTO"""
        waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
        assertLatestRefreshTask("PARTIAL", "PLAN_PATTERN_UNSUPPORTED", 1)
        assertMvEqualsBase()
        assertStreamLagZero()
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)
    }

    sql """INSERT INTO ivm_fbs_o VALUES (12, 20240101, 1, 18);"""
    waitVisible()
    sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
    assertLatestRefreshTask("INCREMENTAL", null, 1)
    assertMvEqualsBase()
    assertStreamLagZero()

    sql """
        INSERT INTO ivm_fbs_c VALUES
            (1, 'alice_v2'),
            (101, 'bob_v2');
    """
    sql """
        INSERT INTO ivm_fbs_o VALUES
            (14, 20240101, 1, 28),
            (21, 20240102, 101, 25);
    """
    waitVisible()

    try {
        GetDebugPoint().enableDebugPointForAllFEs(signatureSaltDebugPoint, [value: "plan_changed"])
        sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv AUTO"""
        waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
        assertLatestRefreshTask("COMPLETE", "PLAN_SIGNATURE_MISMATCH", 2)
        assertMvEqualsBase()
        assertStreamLagZero()

        sql """INSERT INTO ivm_fbs_o VALUES (13, 20240101, 1, 19);"""
        waitVisible()
        sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv INCREMENTAL"""
        waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
        assertLatestRefreshTask("INCREMENTAL", null, 1)
        assertMvEqualsBase()
        assertStreamLagZero()
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(signatureSaltDebugPoint)
    }

    // Write after the first batch captures its RESET boundary. The first batch must not see
    // its late row; the second batch captures later and should see the other late row.
    sql """
        INSERT INTO ivm_fbs_o VALUES
            (30, 20240101, 1, 30),
            (40, 20240102, 101, 40);
    """
    waitVisible()
    try {
        // Disabling the signature salt above makes the persisted salted signature differ
        // from the current layout, so this exercises the real mismatch payload.
        GetDebugPoint().enableDebugPointForAllFEs(blockAfterCaptureDebugPoint)
        sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv AUTO"""
        waitForLatestTask({ task -> task[1].toString() == "RUNNING"
                && task[6].toString() == blockAfterCaptureDebugPoint }, "refresh blocked after capture")

        sql """
            INSERT INTO ivm_fbs_o VALUES
                (31, 20240101, 1, 31),
                (41, 20240102, 101, 41);
        """
        waitVisible()
        GetDebugPoint().disableDebugPointForAllFEs(blockAfterCaptureDebugPoint)

        waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
        assertLatestRefreshTask("COMPLETE", "PLAN_SIGNATURE_MISMATCH", 2)
        def lateRowsInMv = sql """
            SELECT order_id FROM ivm_fbs_mb_mv
            WHERE order_id IN (31, 41)
            ORDER BY order_id
        """
        assertEquals(1, lateRowsInMv.size())
        assertOneFactPartitionHasLag()
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(blockAfterCaptureDebugPoint)
    }

    sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
    assertLatestRefreshTask("INCREMENTAL", null, 1)
    assertMvEqualsBase()
    assertStreamLagZero()

    // Fail the second COMPLETE batch after the first batch has committed. The next AUTO
    // refresh must recover the remaining stream lag and produce a consistent full image.
    sql """
        INSERT INTO ivm_fbs_o VALUES
            (50, 20240101, 1, 50),
            (60, 20240102, 101, 60);
    """
    waitVisible()
    try {
        GetDebugPoint().enableDebugPointForAllFEs(forcedFallbackDebugPoint,
                [reason: "BINLOG_BROKEN"])
        GetDebugPoint().enableDebugPointForAllFEs(failBatchDebugPoint, [batch_index: "1"])
        sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv AUTO"""

        def failedTask = waitForLatestTask({ task -> task[1].toString() == "FAILED" },
                "second refresh batch failure")
        assertEquals("COMPLETE", failedTask[2].toString())
        assertEquals("2", failedTask[3].toString())
        assertEquals("1", failedTask[4].toString())
        assertEquals("50.00% (1/2)", failedTask[5].toString())
        assertTrue(failedTask[7].toString().contains("Forced MTMV refresh batch failure, batch=1"))
        assertEquals("BINLOG_BROKEN", failedTask[8].toString())
        assertOneFactPartitionHasLag()

        GetDebugPoint().disableDebugPointForAllFEs(failBatchDebugPoint)
        sql """REFRESH MATERIALIZED VIEW ivm_fbs_mb_mv AUTO"""
        waitingMTMVTaskFinishedByMvName("ivm_fbs_mb_mv")
        assertLatestRefreshTask("COMPLETE", "BINLOG_BROKEN", 2)
        assertMvEqualsBase()
        assertStreamLagZero()
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(failBatchDebugPoint)
        GetDebugPoint().disableDebugPointForAllFEs(forcedFallbackDebugPoint)
    }
}
