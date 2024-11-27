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

package org.apache.doris.nereids.rules.exploration.mv.mapping;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Mapping slot from query to view or inversely,
 * it can also represent the mapping from slot to it's index
 */
public abstract class Mapping {

    /**
     * Permutation and remove duplicated element
     * For example:
     * Given [1, 4, 5] and [191, 194, 195]
     * This would return
     * [
     * [(1, 191) (4, 194) (5, 195)],
     * [(1, 191) (4, 195) (5, 194)],
     * [(1, 194) (4, 191) (5, 195)],
     * [(1, 194) (4, 195) (5, 191)],
     * [(1, 195) (4, 191) (5, 194)],
     * [(1, 195) (4, 194) (5, 191)]
     * ]
     * */
    protected <M> List<BiMap<M, M>> getUniquePermutation(M[] left, M[] right, Class<M> type) {
        boolean needSwap = left.length > right.length;
        if (needSwap) {
            M[] temp = left;
            left = right;
            right = temp;
        }

        boolean[] used = new boolean[right.length];
        M[] current = (M[]) Array.newInstance(type, left.length);
        List<Pair<M[], M[]>> results = new ArrayList<>();
        backtrack(left, right, 0, used, current, results);
        if (needSwap) {
            List<Pair<M[], M[]>> tmpResults = results;
            results = new ArrayList<>();
            for (Pair<M[], M[]> relation : tmpResults) {
                results.add(Pair.of(relation.value(), relation.key()));
            }
        }
        List<BiMap<M, M>> mappingPowerList = new ArrayList<>();
        for (Pair<M[], M[]> combination : results) {
            BiMap<M, M> combinationBiMap = HashBiMap.create();
            M[] key = combination.key();
            M[] value = combination.value();
            int length = Math.min(key.length, value.length);
            for (int i = 0; i < length; i++) {
                combinationBiMap.put(key[i], value[i]);
            }
            mappingPowerList.add(combinationBiMap);
        }
        return mappingPowerList;
    }

    protected <M> void backtrack(M[] left, M[] right, int index,
            boolean[] used, M[] current, List<Pair<M[], M[]>> results) {
        if (index == left.length) {
            results.add(Pair.of(Arrays.copyOf(left, left.length), Arrays.copyOf(current, current.length)));
            return;
        }

        for (int i = 0; i < right.length; i++) {
            if (!used[i]) {
                used[i] = true;
                current[index] = right[i];
                backtrack(left, right, index + 1, used, current, results);
                used[i] = false;
            }
        }
    }

    /**
     * The relation for mapping
     */
    public static final class MappedRelation {

        public final RelationId relationId;
        public final CatalogRelation belongedRelation;
        // Generate eagerly, will be used to generate slot mapping
        private final Map<List<String>, Slot> slotNameToSlotMap = new HashMap<>();

        /**
         * Construct relation and slot map
         */
        public MappedRelation(RelationId relationId, CatalogRelation belongedRelation) {
            this.relationId = relationId;
            this.belongedRelation = belongedRelation;
            for (Slot slot : belongedRelation.getOutput()) {
                if (slot instanceof SlotReference) {
                    // variant slot
                    List<String> slotNames = new ArrayList<>();
                    slotNames.add(slot.getName());
                    slotNames.addAll(((SlotReference) slot).getSubPath());
                    slotNameToSlotMap.put(slotNames, slot);
                } else {
                    slotNameToSlotMap.put(ImmutableList.of(slot.getName()), slot);
                }
            }
        }

        public static MappedRelation of(RelationId relationId, CatalogRelation belongedRelation) {
            return new MappedRelation(relationId, belongedRelation);
        }

        public RelationId getRelationId() {
            return relationId;
        }

        public CatalogRelation getBelongedRelation() {
            return belongedRelation;
        }

        public Map<List<String>, Slot> getSlotNameToSlotMap() {
            return slotNameToSlotMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MappedRelation that = (MappedRelation) o;
            return Objects.equals(relationId, that.relationId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(relationId);
        }

        @Override
        public String toString() {
            return "MappedRelation{" + "relationId=" + relationId + ", slotNameToSlotMap=" + slotNameToSlotMap + '}';
        }
    }

    /**
     * The slot for mapping
     */
    public static final class MappedSlot {

        public final ExprId exprId;
        public final Slot slot;
        @Nullable
        public final CatalogRelation belongedRelation;

        public MappedSlot(ExprId exprId,
                Slot slot,
                CatalogRelation belongedRelation) {
            this.exprId = exprId;
            this.slot = slot;
            this.belongedRelation = belongedRelation;
        }

        public static MappedSlot of(ExprId exprId,
                Slot slot,
                CatalogRelation belongedRelation) {
            return new MappedSlot(exprId, slot, belongedRelation);
        }

        public static MappedSlot of(Slot slot,
                CatalogRelation belongedRelation) {
            return new MappedSlot(slot.getExprId(), slot, belongedRelation);
        }

        public static MappedSlot of(Slot slot) {
            return new MappedSlot(slot.getExprId(), slot, null);
        }

        public ExprId getExprId() {
            return exprId;
        }

        public CatalogRelation getBelongedRelation() {
            return belongedRelation;
        }

        public Slot getSlot() {
            return slot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MappedSlot that = (MappedSlot) o;
            return Objects.equals(exprId, that.exprId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exprId);
        }

        @Override
        public String toString() {
            return "MappedSlot{" + "slot=" + slot + '}';
        }
    }
}
