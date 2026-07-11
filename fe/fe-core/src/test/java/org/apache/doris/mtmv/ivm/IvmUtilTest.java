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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash3128;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

class IvmUtilTest {

    private SlotReference intSlot(String name, boolean nullable) {
        return new SlotReference(name, IntegerType.INSTANCE, nullable);
    }

    private SlotReference varcharSlot(String name, boolean nullable) {
        return new SlotReference(name, VarcharType.SYSTEM_DEFAULT, nullable);
    }

    private MurmurHash3128 rowIdHash(Expression result) {
        Assertions.assertInstanceOf(MurmurHash3128.class, result);
        return (MurmurHash3128) result;
    }

    @Test
    void testBuildRowIdHashEmptyReturnsZero() {
        Expression result = IvmUtil.buildRowIdHash(Collections.emptyList());
        Assertions.assertInstanceOf(LargeIntLiteral.class, result);
        Assertions.assertEquals(0L, ((LargeIntLiteral) result).getValue().longValue());
    }

    @Test
    void testBuildRowIdHashSingleKeyStructure() {
        Expression result = IvmUtil.buildRowIdHash(ImmutableList.of(intSlot("k1", false)));
        Assertions.assertEquals(LargeIntType.INSTANCE, result.getDataType());
        MurmurHash3128 hash = rowIdHash(result);
        Assertions.assertEquals(2, hash.arity());
    }

    @Test
    void testBuildRowIdHashMultipleKeysStructure() {
        List<Expression> keys = ImmutableList.of(intSlot("k1", false), intSlot("k2", true));
        Expression result = IvmUtil.buildRowIdHash(keys);
        MurmurHash3128 hash = rowIdHash(result);
        // 2 keys * 2 args each = 4 hash arguments
        Assertions.assertEquals(4, hash.arity());
    }

    @Test
    void testBuildRowIdHashContainsNvlAndIsNull() {
        List<Expression> keys = ImmutableList.of(intSlot("k1", true), intSlot("k2", true));
        Expression result = IvmUtil.buildRowIdHash(keys);
        MurmurHash3128 hash = rowIdHash(result);
        // Even-indexed args (0, 2) should be Nvl; odd-indexed (1, 3) should be Cast(IsNull)
        Assertions.assertInstanceOf(Nvl.class, hash.child(0));
        Assertions.assertInstanceOf(Cast.class, hash.child(1));
        Assertions.assertInstanceOf(IsNull.class, ((Cast) hash.child(1)).child());
        Assertions.assertInstanceOf(Nvl.class, hash.child(2));
        Assertions.assertInstanceOf(Cast.class, hash.child(3));
        Assertions.assertInstanceOf(IsNull.class, ((Cast) hash.child(3)).child());
    }

    @Test
    void testBuildRowIdHashVarcharKeySkipsInnerCast() {
        Expression result = IvmUtil.buildRowIdHash(ImmutableList.of(varcharSlot("k1", false)));
        MurmurHash3128 hash = rowIdHash(result);
        Nvl nvl = (Nvl) hash.child(0);
        // VARCHAR key should not have inner Cast — Nvl wraps the slot directly
        Assertions.assertInstanceOf(SlotReference.class, nvl.child(0));
    }

    @Test
    void testBuildRowIdHashNonVarcharKeyHasInnerCast() {
        Expression result = IvmUtil.buildRowIdHash(ImmutableList.of(intSlot("k1", false)));
        MurmurHash3128 hash = rowIdHash(result);
        Nvl nvl = (Nvl) hash.child(0);
        // INT key should have Cast(slot, VARCHAR) inside Nvl
        Assertions.assertInstanceOf(Cast.class, nvl.child(0));
    }

    // ==================== streamName tests ====================

    @Test
    void testStreamName() {
        MTMV mtmv = mockMtmv(123L, "sales_mv");
        OlapTable firstBase = mockBaseTable(456L);
        OlapTable secondBase = mockBaseTable(789L);

        Assertions.assertEquals("__doris_ivm_stream_sales_mv_3f_co", IvmUtil.streamName(mtmv, firstBase));
        Assertions.assertNotEquals(IvmUtil.streamName(mtmv, firstBase), IvmUtil.streamName(mtmv, secondBase));
    }

    @Test
    void testStreamNamePrefixConsistency() {
        String name = IvmUtil.streamName(mockMtmv(1L, "mv"), mockBaseTable(2L));
        Assertions.assertTrue(name.startsWith(IvmUtil.IVM_STREAM_PREFIX));
    }

    @Test
    void testStreamNameRespectsLengthLimit() {
        int originalLimit = Config.table_name_length_limit;
        try {
            Config.table_name_length_limit = 32;
            String name = IvmUtil.streamName(mockMtmv(1L, "a_very_long_materialized_view_name"),
                    mockBaseTable(2L));

            Assertions.assertEquals(Config.table_name_length_limit, name.length());
            Assertions.assertTrue(name.endsWith("_1_2"));
        } finally {
            Config.table_name_length_limit = originalLimit;
        }
    }

    @Test
    void testStreamNameDoesNotSplitUnicodeSurrogatePair() {
        int originalLimit = Config.table_name_length_limit;
        try {
            int readableLength = 2;
            Config.table_name_length_limit = IvmUtil.IVM_STREAM_PREFIX.length() + readableLength + "_1_2".length();

            String name = IvmUtil.streamName(mockMtmv(1L, "a\ud83d\ude00b"), mockBaseTable(2L));

            Assertions.assertEquals(IvmUtil.IVM_STREAM_PREFIX + "a_1_2", name);
        } finally {
            Config.table_name_length_limit = originalLimit;
        }
    }

    // ==================== buildRowIdHash tests ====================

    @Test
    void testBuildRowIdHashResultNotNullable() {
        // With non-nullable keys
        Expression result1 = IvmUtil.buildRowIdHash(ImmutableList.of(intSlot("k1", false)));
        Assertions.assertFalse(result1.nullable());
        // With nullable keys — result should still be non-nullable due to ifnull/isnull wrapping
        Expression result2 = IvmUtil.buildRowIdHash(ImmutableList.of(intSlot("k1", true)));
        Assertions.assertFalse(result2.nullable());
    }

    private MTMV mockMtmv(long id, String name) {
        MTMV mtmv = Mockito.mock(MTMV.class);
        Mockito.when(mtmv.getId()).thenReturn(id);
        Mockito.when(mtmv.getName()).thenReturn(name);
        return mtmv;
    }

    private OlapTable mockBaseTable(long id) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(id);
        return table;
    }
}
