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

package org.apache.doris.nereids.trees.metadata;

import org.apache.doris.nereids.rules.rewrite.mv.Mapping;
import org.apache.doris.nereids.rules.rewrite.mv.Mapping.IntPair;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * EquivalenceClass
 */
public class EquivalenceClass {

    private final Map<SlotReference, Set<SlotReference>> equivalenceSlotMap = new LinkedHashMap<>();

    public EquivalenceClass() {
    }

    /**
     * EquivalenceClass
     */
    public void addEquivalenceClass(SlotReference slot0, SlotReference slot1) {

        Set<SlotReference> slot0Sets = equivalenceSlotMap.get(slot0);
        Set<SlotReference> slot1Sets = equivalenceSlotMap.get(slot1);
        if (slot0Sets != null && slot1Sets != null) {
            // Both present, we need to merge
            if (slot0Sets.size() < slot1Sets.size()) {
                // We swap them to merge
                Set<SlotReference> tmp = slot1Sets;
                slot1Sets = slot0Sets;
                slot0Sets = tmp;
            }
            for (SlotReference newRef : slot1Sets) {
                slot0Sets.add(newRef);
                equivalenceSlotMap.put(newRef, slot0Sets);
            }
        } else if (slot0Sets != null) {
            // p1 present, we need to merge into it
            slot0Sets.add(slot1);
            equivalenceSlotMap.put(slot1, slot0Sets);
        } else if (slot1Sets != null) {
            // p2 present, we need to merge into it
            slot1Sets.add(slot0);
            equivalenceSlotMap.put(slot0, slot1Sets);
        } else {
            // None are present, add to same equivalence class
            Set<SlotReference> equivalenceClass = new LinkedHashSet<>();
            equivalenceClass.add(slot0);
            equivalenceClass.add(slot1);
            equivalenceSlotMap.put(slot0, equivalenceClass);
            equivalenceSlotMap.put(slot1, equivalenceClass);
        }
    }

    public Map<SlotReference, Set<SlotReference>> getEquivalenceSlotMap() {
        return equivalenceSlotMap;
    }

    public boolean isEmpty() {
        return equivalenceSlotMap.isEmpty();
    }

    /**
     * EquivalenceClass
     */
    public List<Set<SlotReference>> getEquivalenceValues() {
        List<Set<SlotReference>> values = new ArrayList<>();
        equivalenceSlotMap.values().forEach(each -> values.add(each));
        return values;
    }

    /**
     * EquivalenceClass
     */
    public Mapping generateMapping(EquivalenceClass target) {

        List<Set<SlotReference>> sourceEquivalenceValues = this.getEquivalenceValues();
        List<Set<SlotReference>> targetEquivalenceValues = target.getEquivalenceValues();
        Mapping mapping = Mapping.of(sourceEquivalenceValues.size(), targetEquivalenceValues.size());

        for (int i = 0; i < targetEquivalenceValues.size(); i++) {
            boolean foundQueryEquivalenceClass = false;
            final Set<SlotReference> viewEquivalenceClass = targetEquivalenceValues.get(i);
            for (int j = 0; j < sourceEquivalenceValues.size(); j++) {
                final Set<SlotReference> queryEquivalenceClass = sourceEquivalenceValues.get(j);
                if (queryEquivalenceClass.containsAll(viewEquivalenceClass)) {
                    mapping.addMapping(IntPair.of(j, i));
                    foundQueryEquivalenceClass = true;
                    break;
                }
            } // end for
            if (!foundQueryEquivalenceClass) {
                // Target equivalence class not found in source equivalence class
                return null;
            }
        }
        return mapping;
    }
}
