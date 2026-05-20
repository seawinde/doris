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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

class IvmNormalizedPlanSqlGeneratorTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("ivm_sql_poc");
        connectContext.setDatabase("ivm_sql_poc");
        createTable("CREATE TABLE ivm_sql_poc.t1 ("
                + "id INT NOT NULL, v INT NULL, name VARCHAR(32) NULL"
                + ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createTable("CREATE TABLE ivm_sql_poc.t2 ("
                + "id INT NOT NULL, v2 INT NULL"
                + ") DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 "
                + "PROPERTIES('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
    }

    @Test
    void testProjectScanSqlCanBeAnalyzed() {
        assertGeneratedSqlCanBeAnalyzed("select id, v + 1 as v1 from t1");
    }

    @Test
    void testFilterSqlCanBeAnalyzed() {
        assertGeneratedSqlCanBeAnalyzed("select id, v from t1 where v > 10 and name is not null");
    }

    @Test
    void testJoinSqlCanBeAnalyzed() {
        assertGeneratedSqlCanBeAnalyzed("select t1.id, t2.v2 from t1 join t2 on t1.id = t2.id");
    }

    @Test
    void testLeftOuterJoinSqlCanBeAnalyzed() {
        assertAnalyzedPlanSqlCanBeAnalyzed("select t1.id, t2.v2 from t1 left join t2 on t1.id = t2.id");
    }

    @Test
    void testSelfJoinSqlCanBeAnalyzed() {
        assertAnalyzedPlanSqlCanBeAnalyzed(
                "select a.id, b.v from t1 a join t1 b on a.id = b.id where a.v < b.v");
    }

    @Test
    void testUnionAllSqlCanBeAnalyzed() {
        assertGeneratedSqlCanBeAnalyzed(
                "select id, v from t1 where id < 10 union all select id, v from t1 where id >= 10");
    }

    @Test
    void testAggregateSqlCanBeAnalyzed() {
        assertGeneratedSqlCanBeAnalyzed("select id, count(*) as cnt, sum(v) as sum_v from t1 group by id");
    }

    @Test
    void testLimitSqlCanBeAnalyzed() {
        assertAnalyzedPlanSqlCanBeAnalyzed("select id, v from t1 limit 5");
    }

    private void assertGeneratedSqlCanBeAnalyzed(String sql) {
        Plan normalizedPlan = normalize(sql);
        assertGeneratedSqlCanBeAnalyzed(normalizedPlan);
    }

    private void assertAnalyzedPlanSqlCanBeAnalyzed(String sql) {
        Plan analyzedPlan = analyzeWithoutIvm(sql);
        assertGeneratedSqlCanBeAnalyzed(analyzedPlan);
    }

    private void assertGeneratedSqlCanBeAnalyzed(Plan normalizedPlan) {
        String generatedSql = new IvmNormalizedPlanSqlGenerator().generate(normalizedPlan);
        Plan analyzedPlan = analyzeWithoutIvm(generatedSql);

        Assertions.assertEquals(outputNames(normalizedPlan), outputNames(analyzedPlan), generatedSql);
        Assertions.assertEquals(normalizedPlan.getOutput().size(), analyzedPlan.getOutput().size(), generatedSql);
    }

    private Plan normalize(String sql) {
        connectContext.getSessionVariable().setEnableIvmNormalRewrite(true);
        PlanChecker checker = PlanChecker.from(connectContext).analyze(sql);
        CascadesContext cascadesContext = checker.getCascadesContext();
        Assertions.assertTrue(cascadesContext.getIvmNormalizeResult().isPresent());
        return cascadesContext.getIvmNormalizeResult().get().getNormalizedPlan();
    }

    private Plan analyzeWithoutIvm(String sql) {
        try {
            ConnectContext sqlContext = createDefaultCtx();
            sqlContext.setDatabase("ivm_sql_poc");
            sqlContext.getSessionVariable().setEnableIvmNormalRewrite(false);
            return PlanChecker.from(sqlContext).analyze(sql).getCascadesContext().getRewritePlan();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<String> outputNames(Plan plan) {
        return plan.getOutput().stream().map(Slot::getName).collect(Collectors.toList());
    }
}
