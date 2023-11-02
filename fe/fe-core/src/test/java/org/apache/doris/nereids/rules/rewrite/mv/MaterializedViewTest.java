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

import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MaterializedViewTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("mv_poc");
        useDatabase("mv_poc");

        createTable("CREATE TABLE lineitem (\n"
                + "    l_shipdate    DATE NOT NULL,\n"
                + "    l_orderkey    bigint NOT NULL,\n"
                + "    l_linenumber  int not null,\n"
                + "    l_partkey     int NOT NULL,\n"
                + "    l_suppkey     int not null,\n"
                + "    l_quantity    decimal(15, 2) NOT NULL,\n"
                + "    l_extendedprice  decimal(15, 2) NOT NULL,\n"
                + "    l_discount    decimal(15, 2) NOT NULL,\n"
                + "    l_tax         decimal(15, 2) NOT NULL,\n"
                + "    l_returnflag  VARCHAR(1) NOT NULL,\n"
                + "    l_linestatus  VARCHAR(1) NOT NULL,\n"
                + "    l_commitdate  DATE NOT NULL,\n"
                + "    l_receiptdate DATE NOT NULL,\n"
                + "    l_shipinstruct VARCHAR(25) NOT NULL,\n"
                + "    l_shipmode     VARCHAR(10) NOT NULL,\n"
                + "    l_comment      VARCHAR(44) NOT NULL\n"
                + ")ENGINE=OLAP\n"
                + "DUPLICATE KEY(`l_shipdate`, `l_orderkey`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\",\n"
                + "    \"colocate_with\" = \"lineitem_orders\"\n"
                + ");");

        createTable("CREATE TABLE orders  (\n"
                + "    o_orderkey       bigint NOT NULL,\n"
                + "    o_orderdate      DATE NOT NULL,\n"
                + "    o_custkey        int NOT NULL,\n"
                + "    o_orderstatus    VARCHAR(1) NOT NULL,\n"
                + "    o_totalprice     decimal(15, 2) NOT NULL,\n"
                + "    o_orderpriority  VARCHAR(15) NOT NULL,\n"
                + "    o_clerk          VARCHAR(15) NOT NULL,\n"
                + "    o_shippriority   int NOT NULL,\n"
                + "    o_comment        VARCHAR(79) NOT NULL\n"
                + ")ENGINE=OLAP\n"
                + "DUPLICATE KEY(`o_orderkey`, `o_orderdate`)\n"
                + "COMMENT \"OLAP\"\n"
                + "DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 32\n"
                + "PROPERTIES (\n"
                + "    \"replication_num\" = \"1\",\n"
                + "    \"colocate_with\" = \"lineitem_orders\"\n"
                + ");");

        createTableAsSelect("CREATE TABLE mv PROPERTIES(\"replication_num\" = \"1\") "
                + "as select l_shipdate, l_linenumber from lineitem inner join orders on l_orderkey = o_orderkey;");
    }

    @Test
    public void testInnerJoin() {

        connectContext.getSessionVariable().enableNereidsTimeout = false;
        connectContext.getSessionVariable().enableDPHypOptimizer = true;
        // query only l_orderkey from join(lineitem, orders) will output l_orderkey and o_orderkey
        // PoC just use lineitem's field
        PlanChecker.from(connectContext)
                .checkMVRewrite(
                        "select l_shipdate from lineitem inner join orders on l_orderkey = o_orderkey",
                        "select l_shipdate, l_linenumber from lineitem inner join orders on l_orderkey = o_orderkey",
                        "select l_shipdate, l_linenumber from mv",
                        (queryPlanner, mvPlanner) -> {
                            PhysicalPlan physicalPlan = queryPlanner.getPhysicalPlan();
                            Assertions.assertTrue(
                                    ((PhysicalResultSink) physicalPlan).toJson().toString().contains("mv"));
                        }
                );
    }
}
