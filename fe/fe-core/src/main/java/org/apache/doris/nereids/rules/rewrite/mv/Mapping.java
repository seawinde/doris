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

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * mapping query to view transversely
 * and mapping current node to input node vertically
 */
public class Mapping {

    private final int sourceCount;
    private final int targetCount;
    // TODO more efficient implement
    private final List<IntPair> mappings;

    public Mapping(int sourceCount, int targetCount, List<IntPair> mappings) {
        this.sourceCount = sourceCount;
        this.targetCount = targetCount;
        this.mappings = mappings == null ? new ArrayList<>() : mappings;
    }

    public int getSourceCount() {
        return sourceCount;
    }

    public int getTargetCount() {
        return targetCount;
    }

    public List<IntPair> getMappings() {
        return mappings;
    }

    /**
     * Mapping
     */
    public static Mapping of(int sourceCount, int targetCount, List<IntPair> mappings) {
        return new Mapping(sourceCount, targetCount, mappings);
    }

    public static Mapping of(int sourceCount, int targetCount) {
        return new Mapping(sourceCount, targetCount, new ArrayList<>());
    }

    public static Mapping empty() {
        return new Mapping(0, 0, ImmutableList.of());
    }

    public void addMapping(IntPair pair) {
        this.mappings.add(pair);
    }

    public boolean isEmpty() {
        return sourceCount == 0 && targetCount == 0 && this.mappings.isEmpty();
    }

    public List<Integer> getTargetBy(int source) {
        return mappings.stream()
                .filter(pair -> pair.getSource() == source)
                .map(IntPair::getTarget)
                .collect(Collectors.toList());
    }

    public List<Integer> getTarget() {
        return mappings.stream().map(IntPair::getTarget).collect(Collectors.toList());
    }

    public List<Integer> getSource() {
        return mappings.stream().map(IntPair::getSource).collect(Collectors.toList());
    }

    /**
     * IntPair
     */
    public static final class IntPair {
        public final int source;
        public final int target;

        public IntPair(int source, int target) {
            this.source = source;
            this.target = target;
        }

        public static IntPair of(int left, int right) {
            return new IntPair(left, right);
        }

        public int getSource() {
            return source;
        }

        public int getTarget() {
            return target;
        }
    }
}
