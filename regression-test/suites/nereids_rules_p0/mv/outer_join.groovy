
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

suite("outer_join") {
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
    (2, 2, 'ok', 109.2, '2023-12-09', 'c','d',2, 'mm');  
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

    // select + from + left outer join
    def mv1_0 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query1_0 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    order_qt_query1_0_before "${query1_0}"
    check_rewrite(mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""


    def mv1_1 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY, partsupp.PS_AVAILQTY " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "left join partsupp on lineitem.L_PARTKEY = partsupp.PS_PARTKEY " +
            "and lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY"
    def query1_1 = "select  lineitem.L_LINENUMBER " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "left join partsupp on lineitem.L_PARTKEY = partsupp.PS_PARTKEY " +
            "and lineitem.L_SUPPKEY = partsupp.PS_SUPPKEY"
    order_qt_query1_1_before "${query1_1}"
    check_rewrite(mv1_1, query1_1, "mv1_1")
    order_qt_query1_1_after "${query1_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_1"""


    def mv1_2 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY " +
            "from orders " +
            "left join lineitem on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query1_2 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    order_qt_query1_2_before "${query1_2}"
    check_not_match(mv1_2, query1_2, "mv1_2")
    order_qt_query1_2_after "${query1_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_2"""

    // select with complex expression + from + inner join
    def mv1_3 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY " +
            "from orders " +
            "left join lineitem on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query1_3 = "select IFNULL(orders.O_CUSTKEY, 0) as custkey_not_null " +
            "from orders " +
            "left join lineitem on orders.O_ORDERKEY = lineitem.L_ORDERKEY"
    order_qt_query1_3_before "${query1_3}"
    check_rewrite(mv1_3, query1_3, "mv1_3")
    order_qt_query1_3_after "${query1_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_3"""


    // select + from + left outer join + filter
    def mv2_0 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY " +
            "from orders " +
            "left join lineitem on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query2_0 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where lineitem.L_LINENUMBER > 10"
    order_qt_query2_0_before "${query2_0}"
    // should not match, because the join direction is not same
    check_not_match(mv2_0, query2_0, "mv2_0")
    order_qt_query2_0_after "${query2_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_0"""


    def mv2_1 = "select  lineitem.L_LINENUMBER, orders.O_CUSTKEY " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query2_1 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERSTATUS = 'ok'"
    order_qt_query2_1_before "${query2_1}"
    // use a filed not from mv, should not success
    check_not_match(mv2_1, query2_1, "mv2_1")
    order_qt_query2_1_after "${query2_1}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_1"""


    def mv2_2 = "select  t1.L_LINENUMBER, orders.O_CUSTKEY " +
            "from (select * from lineitem where L_LINENUMBER > 1) t1 " +
            "left join orders on t1.L_ORDERKEY = orders.O_ORDERKEY "
    def query2_2 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where lineitem.L_LINENUMBER > 1"
    order_qt_query2_2_before "${query2_2}"
    check_rewrite(mv2_2, query2_2, "mv2_2")
    order_qt_query2_2_after "${query2_2}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_2"""


    def mv2_3 = "select lineitem.L_LINENUMBER, orders.O_CUSTKEY, orders.O_ORDERSTATUS " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY "
    def query2_3 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERSTATUS = 'ok'"
    order_qt_query2_3_before "${query2_3}"
    // left outer -> inner jon because orders.O_ORDERSTATUS = 'ok', but mv is left join, will fix in the future
//    check_rewrite(mv2_3, query2_3, "mv2_3")
    order_qt_query2_3_after "${query2_3}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_3"""


    def mv2_4 = "select lineitem.L_LINENUMBER, t2.O_CUSTKEY, t2.O_ORDERSTATUS " +
            "from lineitem " +
            "left join " +
            "(select * from orders where O_ORDERSTATUS = 'ok') t2 " +
            "on lineitem.L_ORDERKEY = t2.O_ORDERKEY "
    def query2_4 = "select lineitem.L_LINENUMBER " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where orders.O_ORDERSTATUS = 'ok'"
    order_qt_query2_4_before "${query2_4}"
    // should not success, as mv filter is under left outer input
    check_not_match(mv2_4, query2_4, "mv2_4")
    order_qt_query2_4_after "${query2_4}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_4"""


    def mv2_5 = "select lineitem.L_LINENUMBER, orders.O_CUSTKEY " +
            "from lineitem " +
            "left join orders on lineitem.L_ORDERKEY = orders.O_ORDERKEY " +
            "where lineitem.L_LINENUMBER > 1"
    def query2_5 = "select  t1.L_LINENUMBER " +
            "from (select * from lineitem where L_LINENUMBER > 1) t1 " +
            "left join orders on t1.L_ORDERKEY = orders.O_ORDERKEY "
    order_qt_query2_5_before "${query2_5}"
    check_rewrite(mv2_5, query2_5, "mv2_5")
    order_qt_query2_5_after "${query2_5}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv2_5"""
}
