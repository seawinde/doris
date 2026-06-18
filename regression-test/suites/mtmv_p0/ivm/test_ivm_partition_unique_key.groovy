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

suite("test_ivm_partition_unique_key") {
    sql """drop materialized view if exists mv_ivm_partition_key;"""
    sql """drop materialized view if exists mv_ivm_partition_auto_key;"""
    sql """drop materialized view if exists mv_ivm_partition_bad_key;"""
    sql """drop materialized view if exists mv_ivm_partition_bad_dist;"""
    sql """drop materialized view if exists mv_ivm_partition_bad_agg_key;"""
    sql """drop materialized view if exists mv_ivm_partition_bad_agg_value_key;"""
    sql """drop table if exists t_ivm_partition_key_base;"""

    sql """
        CREATE TABLE t_ivm_partition_key_base (
            id INT,
            dt DATE,
            v INT
        )
        UNIQUE KEY(id, dt)
        PARTITION BY RANGE(dt) (
            PARTITION p20260601 VALUES [('2026-06-01'), ('2026-06-02')),
            PARTITION p20260602 VALUES [('2026-06-02'), ('2026-06-03'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO t_ivm_partition_key_base VALUES
            (1, '2026-06-01', 10),
            (2, '2026-06-01', 20),
            (3, '2026-06-02', 30);
    """

    sql """
        CREATE MATERIALIZED VIEW mv_ivm_partition_key
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(dt, id)
        PARTITION BY(dt)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT dt, id, v FROM t_ivm_partition_key_base;
    """

    def descResult = sql """desc mv_ivm_partition_key all"""
    assertTrue(descResult.toString().contains("UNIQUE_KEYS"))

    sql """REFRESH MATERIALIZED VIEW mv_ivm_partition_key COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_partition_key")
    order_qt_partition_key_complete """SELECT dt, id, v FROM mv_ivm_partition_key"""

    sql """INSERT INTO t_ivm_partition_key_base VALUES (2, '2026-06-01', 22);"""
    sql """REFRESH MATERIALIZED VIEW mv_ivm_partition_key INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_partition_key")
    order_qt_partition_key_incremental """SELECT dt, id, v FROM mv_ivm_partition_key"""

    sql """
        CREATE MATERIALIZED VIEW mv_ivm_partition_auto_key
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PARTITION BY(dt)
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT dt, id, v FROM t_ivm_partition_key_base;
    """

    def autoDescResult = sql """desc mv_ivm_partition_auto_key all"""
    assertTrue(autoDescResult.toString().contains("UNIQUE_KEYS"))

    sql """REFRESH MATERIALIZED VIEW mv_ivm_partition_auto_key COMPLETE"""
    waitingMTMVTaskFinishedByMvName("mv_ivm_partition_auto_key")
    order_qt_auto_key_complete """SELECT dt, id, v FROM mv_ivm_partition_auto_key"""

    test {
        sql """
            CREATE MATERIALIZED VIEW mv_ivm_partition_bad_key
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            KEY(id)
            PARTITION BY(dt)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                'replication_num' = '1'
            )
            AS SELECT dt, id, v FROM t_ivm_partition_key_base;
        """
        exception "partition column must be key column"
    }

    test {
        sql """
            CREATE MATERIALIZED VIEW mv_ivm_partition_bad_dist
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            KEY(id)
            DISTRIBUTED BY HASH(dt) BUCKETS 2
            PROPERTIES (
                'replication_num' = '1'
            )
            AS SELECT dt, id, v FROM t_ivm_partition_key_base;
        """
        exception "distribution column must be key column"
    }

    test {
        sql """
            CREATE MATERIALIZED VIEW mv_ivm_partition_bad_agg_key
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                'replication_num' = '1'
            )
            AS SELECT dt, id, SUM(v) AS total_v FROM t_ivm_partition_key_base GROUP BY dt, id;
        """
        exception "group key column"
    }

    test {
        sql """
            CREATE MATERIALIZED VIEW mv_ivm_partition_bad_agg_value_key
            BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
            KEY(dt, id, total_v)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                'replication_num' = '1'
            )
            AS SELECT dt, id, SUM(v) AS total_v FROM t_ivm_partition_key_base GROUP BY dt, id;
        """
        exception "aggregate result column"
    }
}
