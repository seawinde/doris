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

suite("aggregate_without_roll_up") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=true"
    sql "SET enable_nereids_timeout = false"
    // tmp disable to rewrite, will be removed in the future
    sql "SET disable_nereids_rules = 'INFER_PREDICATES, ELIMINATE_OUTER_JOIN'"

    sql """
    drop table if exists orders
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders  (
      O_ORDERKEY       INTEGER NOT NULL,
      O_CUSTKEY        INTEGER NOT NULL,
      O_ORDERSTATUS    CHAR(1) NOT NULL,
      O_TOTALPRICE     DECIMALV3(15,2) NOT NULL,
      O_ORDERDATE      DATE NOT NULL,
      O_ORDERPRIORITY  CHAR(15) NOT NULL,  
      O_CLERK          CHAR(15) NOT NULL, 
      O_SHIPPRIORITY   INTEGER NOT NULL,
      O_COMMENT        VARCHAR(79) NOT NULL
    )
    DUPLICATE KEY(O_ORDERKEY, O_CUSTKEY)
    PARTITION BY RANGE(O_ORDERDATE) (PARTITION `day_2` VALUES LESS THAN ('2023-12-30'))
    DISTRIBUTED BY HASH(O_ORDERKEY) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    drop table if exists lineitem
    """

    sql"""
    CREATE TABLE IF NOT EXISTS lineitem (
      L_ORDERKEY    INTEGER NOT NULL,
      L_PARTKEY     INTEGER NOT NULL,
      L_SUPPKEY     INTEGER NOT NULL,
      L_LINENUMBER  INTEGER NOT NULL,
      L_QUANTITY    DECIMALV3(15,2) NOT NULL,
      L_EXTENDEDPRICE  DECIMALV3(15,2) NOT NULL,
      L_DISCOUNT    DECIMALV3(15,2) NOT NULL,
      L_TAX         DECIMALV3(15,2) NOT NULL,
      L_RETURNFLAG  CHAR(1) NOT NULL,
      L_LINESTATUS  CHAR(1) NOT NULL,
      L_SHIPDATE    DATE NOT NULL,
      L_COMMITDATE  DATE NOT NULL,
      L_RECEIPTDATE DATE NOT NULL,
      L_SHIPINSTRUCT CHAR(25) NOT NULL,
      L_SHIPMODE     CHAR(10) NOT NULL,
      L_COMMENT      VARCHAR(44) NOT NULL
    )
    DUPLICATE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER)
    PARTITION BY RANGE(L_SHIPDATE) (PARTITION `day_1` VALUES LESS THAN ('2023-12-30'))
    DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """
    drop table if exists partsupp
    """

    sql """
    CREATE TABLE IF NOT EXISTS partsupp (
      PS_PARTKEY     INTEGER NOT NULL,
      PS_SUPPKEY     INTEGER NOT NULL,
      PS_AVAILQTY    INTEGER NOT NULL,
      PS_SUPPLYCOST  DECIMALV3(15,2)  NOT NULL,
      PS_COMMENT     VARCHAR(199) NOT NULL 
    )
    DUPLICATE KEY(PS_PARTKEY, PS_SUPPKEY)
    DISTRIBUTED BY HASH(PS_PARTKEY) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
    """

    sql """ insert into lineitem values (1, 2, 3, 4, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-12-08', '2023-12-09', '2023-12-10', 'a', 'b', 'yyyyyyyyy'),
    (2, 2, 3, 6, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-12-11', '2023-12-12', '2023-12-13', 'c', 'd', 'xxxxxxxxx');"""

    sql """
    insert into orders values (1, 1, 'ok', 99.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 2, 'ok', 109.2, '2023-12-11', 'c','d',2, 'mm'),
    (1, 2, 'ok', 109.2, '2023-12-11', 'c','d',2, 'mi');  
    """

    sql """
    insert into partsupp values (2, 3, 9, 10.01, 'supply1'),
    (2, 3, 10, 11.01, 'supply2');
    """

    def check_rewrite = { mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            contains "(${mv_name})"
        }
    }

    def check_not_match = { mv_sql, query_sql, mv_name ->

        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name}"""
        sql"""
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS ${mv_sql}
        """

        def job_name = getJobName(db, mv_name);
        waitingMTMVTaskFinished(job_name)
        explain {
            sql("${query_sql}")
            notContains "(${mv_name})"
        }
    }

    // select + from + inner join + group by
    def mv1_0 = "select lineitem.L_LINENUMBER, orders.O_CUSTKEY, sum(O_TOTALPRICE) as sum_alias " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "group by lineitem.L_LINENUMBER, orders.O_CUSTKEY "
    def query1_0 = "select lineitem.L_LINENUMBER, orders.O_CUSTKEY, sum(O_TOTALPRICE) as sum_alias " +
            "from lineitem " +
            "inner join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "group by lineitem.L_LINENUMBER, orders.O_CUSTKEY "
//    order_qt_query1_0_before "${query1_0}"
//    check_rewrite(mv1_0, query1_0, "mv1_0")
//    order_qt_query1_0_after "${query1_0}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    // select case when + from + where + group by
    def mv1_1 = "select O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 15) then O_ORDERSTATUS else null end) as filter_cnt_10, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from orders " +
            "where O_ORDERDATE < '2023-12-30' and O_ORDERDATE > '2023-12-01'" +
            "group by " +
            "O_SHIPPRIORITY, " +
            "O_COMMENT "
    def query1_1 = "select O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from orders " +
            "where O_ORDERDATE < '2023-12-30' and O_ORDERDATE > '2023-12-01'" +
            "group by " +
            "O_SHIPPRIORITY, " +
            "O_COMMENT "
//    order_qt_query1_1_before "${query1_1}"
//    check_rewrite(mv1_1, query1_1, "mv1_1")
//    order_qt_query1_1_after "${query1_1}"
//    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""


    // select case when + from + where + group by
    def mv2_0 = "select L_ORDERKEY, O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (11, 13) then O_ORDERSTATUS else null end) as filter_cnt_8, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (12, 11) then O_ORDERSTATUS else null end) as filter_cnt_9, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 15) then O_ORDERSTATUS else null end) as filter_cnt_10, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (11, 12) then O_ORDERSTATUS else null end) as filter_cnt_11, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 25) then O_ORDERSTATUS else null end) as filter_cnt_18, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (19, 3) then O_ORDERSTATUS else null end) as filter_cnt_19, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 20) then O_ORDERSTATUS else null end) as filter_cnt_20 " +
            "from lineitem " +
            "left join " +
            "orders " +
            "on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERDATE < '2023-12-30' and orders.O_ORDERDATE > '2023-12-01' " +
            "group by " +
            "lineitem.L_ORDERKEY, " +
            "orders.O_SHIPPRIORITY, " +
            "orders.O_COMMENT "
    def query2_0 =  "select L_ORDERKEY, O_SHIPPRIORITY, O_COMMENT, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (1, 3) then O_ORDERSTATUS else null end) as filter_cnt_1, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (2) then O_ORDERSTATUS else null end) as filter_cnt_2, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (3, 4) then O_ORDERSTATUS else null end) as filter_cnt_3, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (5, 6) then O_ORDERSTATUS else null end) as filter_cnt_4, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (2, 3) then O_ORDERSTATUS else null end) as filter_cnt_5, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (7, 9) then O_ORDERSTATUS else null end) as filter_cnt_6, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (8, 10) then O_ORDERSTATUS else null end) as filter_cnt_7, " +
            "count(distinct case when O_SHIPPRIORITY > 4 and O_ORDERKEY IN (3, 6) then O_ORDERSTATUS else null end) as filter_cnt_12, " +
            "count(distinct case when O_SHIPPRIORITY > 3 and O_ORDERKEY IN (16, 19) then O_ORDERSTATUS else null end) as filter_cnt_13, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (20, 3) then O_ORDERSTATUS else null end) as filter_cnt_14, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (15, 19) then O_ORDERSTATUS else null end) as filter_cnt_15, " +
            "count(distinct case when O_SHIPPRIORITY > 1 and O_ORDERKEY IN (13, 21) then O_ORDERSTATUS else null end) as filter_cnt_16, " +
            "count(distinct case when O_SHIPPRIORITY > 2 and O_ORDERKEY IN (14, 22) then O_ORDERSTATUS else null end) as filter_cnt_17 " +
            "from lineitem " +
            "left join " +
            "orders " +
            "on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERDATE < '2023-12-30' and orders.O_ORDERDATE > '2023-12-01' " +
            "group by " +
            "lineitem.L_ORDERKEY, " +
            "orders.O_SHIPPRIORITY, " +
            "orders.O_COMMENT "
    order_qt_query2_0_before "${query2_0}"
    check_rewrite(mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""
}
