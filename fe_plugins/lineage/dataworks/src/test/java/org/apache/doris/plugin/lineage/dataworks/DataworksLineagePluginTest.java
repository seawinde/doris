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

package org.apache.doris.plugin.lineage.dataworks;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.lineage.LineageContext;
import org.apache.doris.nereids.lineage.LineageInfo;
import org.apache.doris.nereids.lineage.LineageInfo.DirectLineageType;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataworksLineagePluginTest {
    private String[] originalPlugins;

    @Before
    public void setUp() {
        originalPlugins = Config.activate_lineage_plugin;
    }

    @After
    public void tearDown() {
        Config.activate_lineage_plugin = originalPlugins;
    }

    @Test
    public void testEventFilterScopes() {
        DataworksLineagePlugin plugin = new DataworksLineagePlugin();

        Config.activate_lineage_plugin = new String[] {"other"};
        Deencapsulation.setField(plugin, "enabledScope", "table");
        Assert.assertFalse(plugin.eventFilter());

        Config.activate_lineage_plugin = new String[] {"dataworks"};
        Deencapsulation.setField(plugin, "enabledScope", "");
        Assert.assertFalse(plugin.eventFilter());

        Deencapsulation.setField(plugin, "enabledScope", "table,column");
        Assert.assertTrue(plugin.eventFilter());
    }

    @Test
    public void testBuildLineageDetailInfo() {
        DataworksLineagePlugin plugin = new DataworksLineagePlugin();
        LineageInfo lineageInfo = new LineageInfo();

        LineageContext context = new LineageContext(InsertIntoTableCommand.class, "q1",
                "insert into tgt select 1", "user1", "lineage_tpch", 10L, 5L);
        context.setClientIp("127.0.0.1:9030");
        context.setState("OK");
        lineageInfo.setContext(context);

        TableIf targetTable = mockTable("tgt_table", "lineage_tpch", "default_catalog");
        lineageInfo.setTargetTable(targetTable);

        TableIf srcTable = mockTable("src_table", "src_db", "default_catalog");
        Set<TableIf> sources = new HashSet<>();
        sources.add(srcTable);
        lineageInfo.setTableLineageSet(sources);

        SlotReference targetSlot = new SlotReference(newExprId(), "col1", IntegerType.INSTANCE,
                true, ImmutableList.of());
        SlotReference sourceSlot = new SlotReference(newExprId(), "src_col", IntegerType.INSTANCE,
                true, ImmutableList.of("src_db", "src_table"), srcTable, null, srcTable, null);
        SlotReference constantSlot = new SlotReference(newExprId(), "const_col", IntegerType.INSTANCE,
                true, ImmutableList.of());

        lineageInfo.addDirectLineage(targetSlot, DirectLineageType.IDENTITY, sourceSlot);
        lineageInfo.addDirectLineage(constantSlot, DirectLineageType.TRANSFORMATION, new IntegerLiteral(1));

        Object scopeFlags = Deencapsulation.invoke(plugin, "parseScopes", "column");
        Object hookInfo = Deencapsulation.invoke(plugin, "buildLineageDetailInfo", lineageInfo, scopeFlags);

        Assert.assertEquals("Lineage", Deencapsulation.getField(hookInfo, "actionType"));
        Assert.assertEquals("InsertIntoTableCommand", Deencapsulation.getField(hookInfo, "action"));
        Assert.assertEquals("SUCCESS", Deencapsulation.getField(hookInfo, "state"));
        Assert.assertEquals("127.0.0.1:9030", Deencapsulation.getField(hookInfo, "clientIp"));

        List<?> columnLineages = Deencapsulation.getField(hookInfo, "columnLineages");
        Assert.assertEquals(1, columnLineages.size());
        Object lineage = columnLineages.get(0);
        Assert.assertEquals("src_db", Deencapsulation.getField(lineage, "srcDatabase"));
        Assert.assertEquals("src_table", Deencapsulation.getField(lineage, "srcTable"));
        @SuppressWarnings("unchecked")
        Map<String, List<String>> columnMap = Deencapsulation.getField(lineage, "columnMap");
        Assert.assertEquals(2, columnMap.size());
        Assert.assertEquals(1, columnMap.get("col1").size());
        Assert.assertTrue(columnMap.get("col1").contains("src_col"));
        Assert.assertTrue(columnMap.get("const_col").isEmpty());
    }

    @Test
    public void testBuildLineageDetailInfoTableOnly() {
        DataworksLineagePlugin plugin = new DataworksLineagePlugin();
        LineageInfo lineageInfo = new LineageInfo();

        LineageContext context = new LineageContext(null, "q2", "insert into t select 1", "user2",
                "lineage_tpch", 10L, 5L);
        lineageInfo.setContext(context);

        TableIf targetTable = mockTable("tgt_table", "lineage_tpch", "default_catalog");
        lineageInfo.setTargetTable(targetTable);
        lineageInfo.setTableLineageSet(new HashSet<>());

        Object scopeFlags = Deencapsulation.invoke(plugin, "parseScopes", "table");
        Object detail = Deencapsulation.invoke(plugin, "buildLineageDetailInfo", lineageInfo, scopeFlags);

        List<?> columnLineages = Deencapsulation.getField(detail, "columnLineages");
        Assert.assertTrue(columnLineages.isEmpty());
    }

    @Test
    public void testBuildLineageDetailInfoStateFailed() {
        DataworksLineagePlugin plugin = new DataworksLineagePlugin();
        LineageInfo lineageInfo = new LineageInfo();

        LineageContext context = new LineageContext(InsertIntoTableCommand.class, "q_state_failed",
                "insert into tgt select 1", "user1", "lineage_tpch", 10L, 5L);
        context.setClientIp("127.0.0.1:9030");
        context.setState("ERR");
        lineageInfo.setContext(context);

        TableIf targetTable = mockTable("tgt_table", "lineage_tpch", "default_catalog");
        lineageInfo.setTargetTable(targetTable);

        TableIf srcTable = mockTable("src_table", "src_db", "default_catalog");
        Set<TableIf> sources = new HashSet<>();
        sources.add(srcTable);
        lineageInfo.setTableLineageSet(sources);

        SlotReference targetSlot = new SlotReference(newExprId(), "col1", IntegerType.INSTANCE,
                true, ImmutableList.of());
        SlotReference sourceSlot = new SlotReference(newExprId(), "src_col", IntegerType.INSTANCE,
                true, ImmutableList.of("src_db", "src_table"), srcTable, null, srcTable, null);
        lineageInfo.addDirectLineage(targetSlot, DirectLineageType.IDENTITY, sourceSlot);

        Object scopeFlags = Deencapsulation.invoke(plugin, "parseScopes", "column");
        Object hookInfo = Deencapsulation.invoke(plugin, "buildLineageDetailInfo", lineageInfo, scopeFlags);

        Assert.assertEquals("FAILED", Deencapsulation.getField(hookInfo, "state"));
    }

    @Test
    public void testBuildLineageDetailInfoColumnOnly() {
        DataworksLineagePlugin plugin = new DataworksLineagePlugin();
        LineageInfo lineageInfo = new LineageInfo();

        LineageContext context = new LineageContext(null, "q3", "insert into t select 1", "user3",
                "lineage_tpch", 10L, 5L);
        lineageInfo.setContext(context);

        lineageInfo.setTargetTable(mockTable("tgt_table", "lineage_tpch", "default_catalog"));
        TableIf srcTable = mockTable("src_table", "src_db", "default_catalog");
        lineageInfo.setTableLineageSet(new HashSet<>(ImmutableList.of(srcTable)));

        SlotReference targetSlot = new SlotReference(newExprId(), "col1", IntegerType.INSTANCE,
                true, ImmutableList.of());
        SlotReference sourceSlot = new SlotReference(newExprId(), "src_col", IntegerType.INSTANCE,
                true, ImmutableList.of("src_db", "src_table"), srcTable, null, srcTable, null);
        lineageInfo.addDirectLineage(targetSlot, DirectLineageType.IDENTITY, sourceSlot);

        Object scopeFlags = Deencapsulation.invoke(plugin, "parseScopes", "column");
        Object detail = Deencapsulation.invoke(plugin, "buildLineageDetailInfo", lineageInfo, scopeFlags);

        List<?> columnLineages = Deencapsulation.getField(detail, "columnLineages");
        Assert.assertEquals(1, columnLineages.size());
    }

    @Test
    public void testProjectionLineageMultipleSources() {
        DataworksLineagePlugin plugin = new DataworksLineagePlugin();
        LineageInfo lineageInfo = new LineageInfo();

        LineageContext context = new LineageContext(null, "q6", "insert into t select 1", "user6",
                "lineage_tpch", 10L, 5L);
        lineageInfo.setContext(context);
        TableIf targetTable = mockTable("tgt_table", "lineage_tpch", "default_catalog");
        lineageInfo.setTargetTable(targetTable);

        TableIf srcTable = mockTable("lineitem", "src_db", "default_catalog");
        lineageInfo.setTableLineageSet(new HashSet<>(ImmutableList.of(srcTable)));

        SlotReference targetSlot = new SlotReference(newExprId(), "net_price", IntegerType.INSTANCE,
                true, ImmutableList.of());
        SlotReference priceSlot = new SlotReference(newExprId(), "l_extendedprice", IntegerType.INSTANCE,
                true, ImmutableList.of("src_db", "lineitem"), srcTable, null, srcTable, null);
        SlotReference discountSlot = new SlotReference(newExprId(), "l_discount", IntegerType.INSTANCE,
                true, ImmutableList.of("src_db", "lineitem"), srcTable, null, srcTable, null);
        lineageInfo.addDirectLineage(targetSlot, DirectLineageType.TRANSFORMATION,
                new Add(priceSlot, discountSlot));

        Object scopeFlags = Deencapsulation.invoke(plugin, "parseScopes", "column");
        Object detail = Deencapsulation.invoke(plugin, "buildLineageDetailInfo", lineageInfo, scopeFlags);

        List<?> columnLineages = Deencapsulation.getField(detail, "columnLineages");
        Assert.assertEquals(1, columnLineages.size());
        Object lineage = columnLineages.get(0);
        @SuppressWarnings("unchecked")
        Map<String, List<String>> columnMap = Deencapsulation.getField(lineage, "columnMap");
        List<String> sources = columnMap.get("net_price");
        Assert.assertNotNull(sources);
        Assert.assertEquals(2, sources.size());
        Assert.assertTrue(sources.contains("l_extendedprice"));
        Assert.assertTrue(sources.contains("l_discount"));
    }

    @Test
    public void testProjectionLineageSplitByDatabase() {
        DataworksLineagePlugin plugin = new DataworksLineagePlugin();
        LineageInfo lineageInfo = new LineageInfo();

        LineageContext context = new LineageContext(null, "q7", "insert into t select 1", "user7",
                "lineage_tpch", 10L, 5L);
        lineageInfo.setContext(context);
        lineageInfo.setTargetTable(mockTable("tgt_table", "lineage_tpch", "default_catalog"));

        TableIf tableA = mockTable("lineitem", "db_a", "default_catalog");
        TableIf tableB = mockTable("lineitem", "db_b", "default_catalog");
        lineageInfo.setTableLineageSet(new HashSet<>(ImmutableList.of(tableA, tableB)));

        SlotReference targetSlot = new SlotReference(newExprId(), "net_price", IntegerType.INSTANCE,
                true, ImmutableList.of());
        SlotReference priceSlot = new SlotReference(newExprId(), "l_extendedprice", IntegerType.INSTANCE,
                true, ImmutableList.of("db_a", "lineitem"), tableA, null, tableA, null);
        SlotReference discountSlot = new SlotReference(newExprId(), "l_discount", IntegerType.INSTANCE,
                true, ImmutableList.of("db_b", "lineitem"), tableB, null, tableB, null);
        lineageInfo.addDirectLineage(targetSlot, DirectLineageType.TRANSFORMATION,
                new Add(priceSlot, discountSlot));

        Object scopeFlags = Deencapsulation.invoke(plugin, "parseScopes", "column");
        Object detail = Deencapsulation.invoke(plugin, "buildLineageDetailInfo", lineageInfo, scopeFlags);

        List<?> columnLineages = Deencapsulation.getField(detail, "columnLineages");
        Assert.assertEquals(2, columnLineages.size());

        Set<String> srcDatabases = new HashSet<>();
        for (Object lineage : columnLineages) {
            srcDatabases.add(Deencapsulation.getField(lineage, "srcDatabase"));
            @SuppressWarnings("unchecked")
            Map<String, List<String>> columnMap = Deencapsulation.getField(lineage, "columnMap");
            Assert.assertEquals(1, columnMap.get("net_price").size());
        }
        Assert.assertTrue(srcDatabases.contains("db_a"));
        Assert.assertTrue(srcDatabases.contains("db_b"));
    }

    @Test
    public void testProjectionLineageDedupSources() {
        DataworksLineagePlugin plugin = new DataworksLineagePlugin();
        LineageInfo lineageInfo = new LineageInfo();

        LineageContext context = new LineageContext(null, "q9", "insert into t select 1", "user9",
                "lineage_tpch", 10L, 5L);
        lineageInfo.setContext(context);
        TableIf targetTable = mockTable("tgt_table", "lineage_tpch", "default_catalog");
        lineageInfo.setTargetTable(targetTable);

        TableIf srcTable = mockTable("lineitem", "src_db", "default_catalog");
        lineageInfo.setTableLineageSet(new HashSet<>(ImmutableList.of(srcTable)));

        SlotReference targetSlot = new SlotReference(newExprId(), "net_price", IntegerType.INSTANCE,
                true, ImmutableList.of());
        SlotReference priceSlot = new SlotReference(newExprId(), "l_extendedprice", IntegerType.INSTANCE,
                true, ImmutableList.of("src_db", "lineitem"), srcTable, null, srcTable, null);
        lineageInfo.addDirectLineage(targetSlot, DirectLineageType.TRANSFORMATION,
                new Add(priceSlot, priceSlot));

        Object scopeFlags = Deencapsulation.invoke(plugin, "parseScopes", "column");
        Object detail = Deencapsulation.invoke(plugin, "buildLineageDetailInfo", lineageInfo, scopeFlags);

        List<?> columnLineages = Deencapsulation.getField(detail, "columnLineages");
        Assert.assertEquals(1, columnLineages.size());
        Object lineage = columnLineages.get(0);
        @SuppressWarnings("unchecked")
        Map<String, List<String>> columnMap = Deencapsulation.getField(lineage, "columnMap");
        Assert.assertEquals(1, columnMap.get("net_price").size());
    }

    private static ExprId newExprId() {
        return StatementScopeIdGenerator.newExprId();
    }

    @SuppressWarnings("unchecked")
    private static TableIf mockTable(String tableName, String dbName, String catalogName) {
        TableIf table = Mockito.mock(TableIf.class);
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn(catalogName);
        Mockito.when(database.getFullName()).thenReturn(dbName);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getDatabase()).thenReturn(database);
        return table;
    }
}
