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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.Util;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

// runtime-only class for unified query/insert experience, created when bind relation with OlapTableStream
public class OlapTableStreamWrapper extends OlapTable {
    private final OlapTableStream stream;
    private final OlapTable baseTable;
    // Unsigned range view used while normalizing stream scans.
    protected final Map<Long, Pair<Long, Long>> outputUpdateMap;
    // Exact signed prev/next update attached to the INSERT transaction.
    private final OlapTableStreamUpdate outputUpdate;
    // Captured RESET/SNAPSHOT wrappers must not recompute their boundary from live stream state.
    private final boolean capturedUpdate;
    private final KeysType keysType;

    public OlapTableStreamWrapper(OlapTableStream stream, OlapTable baseTable, List<Long> selectedPartitionIds) {
        this(stream, baseTable, selectedPartitionIds, null);
    }

    public OlapTableStreamWrapper(OlapTableStream stream, OlapTable baseTable, List<Long> selectedPartitionIds,
            OlapTableStreamUpdate capturedUpdate) {
        super(stream.getId(), stream.getName(), stream.getFullSchema(), baseTable.getKeysType(),
                baseTable.getPartitionInfo(), baseTable.getDefaultDistributionInfo());
        // Inherit base table's qualifiedDbName so that wrapper.getDatabase() can resolve the
        // owning Database via Env.getCurrentInternalCatalog().getDbNullable(qualifiedDbName).
        // Otherwise downstream consumers (e.g. QueryPartitionCollector, partition routing,
        // MV partition compensation) treat the wrapper as having no database and silently
        // fall back to empty results when scanning the stream.
        setQualifiedDbName(baseTable.getQualifiedDbName());
        this.stream = stream;
        this.baseTable = baseTable;
        this.keysType = baseTable.getKeysType();
        this.capturedUpdate = capturedUpdate != null;
        this.outputUpdate = capturedUpdate == null
                ? buildOutputUpdate(selectedPartitionIds) : new OlapTableStreamUpdate(capturedUpdate);
        this.outputUpdateMap = toOutputUpdateMap(this.outputUpdate);
        this.getOrCreatTableProperty().setEnableUniqueKeyMergeOnWrite(baseTable.getEnableUniqueKeyMergeOnWrite());
    }

    private OlapTableStreamUpdate buildOutputUpdate(List<Long> selectedPartitionIds) {
        Map<Long, Long> prev = Maps.newHashMapWithExpectedSize(selectedPartitionIds.size());
        Map<Long, Long> next = Maps.newHashMapWithExpectedSize(selectedPartitionIds.size());
        for (Long partitionId : selectedPartitionIds) {
            if (!baseTable.getPartition(partitionId).hasData()) {
                continue;
            }
            Pair<Long, Long> update = stream.getStreamUpdate(partitionId);
            if (update.first != null) {
                prev.put(partitionId, stream.hasHistoricalData(partitionId) ? -update.first : update.first);
            }
            next.put(partitionId, update.second);
        }
        return new OlapTableStreamUpdate(prev, next);
    }

    private Map<Long, Pair<Long, Long>> toOutputUpdateMap(OlapTableStreamUpdate update) {
        Map<Long, Pair<Long, Long>> updateMap = Maps.newHashMapWithExpectedSize(update.getNext().size());
        for (Map.Entry<Long, Long> entry : update.getNext().entrySet()) {
            Long prev = update.getPrev().get(entry.getKey());
            updateMap.put(entry.getKey(), Pair.of(prev == null ? null : Math.abs(prev), entry.getValue()));
        }
        return updateMap;
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return baseTable.getBaseSchema(full);
    }

    @Override
    public List<Column> getBaseSchema() {
        return baseTable.getBaseSchema();
    }

    // for display
    public String getIndexNameById(long indexId) {
        // always returns base index name
        return baseTable.getName();
    }

    // for olap table to thrift
    @Override
    public void getColumnDesc(long selectedIndexId, List<TColumn> columnsDesc, List<String> keyColumnNames,
                              List<TPrimitiveType> keyColumnTypes) {
        baseTable.getColumnDesc(selectedIndexId, columnsDesc, keyColumnNames, keyColumnTypes);
    }

    @Override
    public int getIndexSchemaVersion(long indexId) {
        return baseTable.getIndexSchemaVersion(indexId);
    }

    // no need for pre agg on olap table stream
    @Override
    public boolean isDupKeysOrMergeOnWrite() {
        return false;
    }

    @Override
    public long getBaseIndexId() {
        return baseTable.getBaseIndexId();
    }

    @Override
    public MaterializedIndexMeta getIndexMetaByIndexId(long indexId) {
        return baseTable.getIndexMetaByIndexId(indexId);
    }

    @Override
    public List<Column> getSchemaByIndexId(Long indexId) {
        // here is base table indexId, we can ignore it and use olap table stream schema
        return getBaseSchema(Util.showHiddenColumns());
    }

    // override all partition methods, olap table stream inherit all partitions from base table
    @Override
    public Partition getPartition(String partitionName) {
        return baseTable.getPartition(partitionName);
    }

    @Override
    public Partition getPartition(long partitionId) {
        return baseTable.getPartition(partitionId);
    }

    @Override
    public Partition getPartition(String partitionName, boolean isTempPartition) {
        return baseTable.getPartition(partitionName, isTempPartition);
    }

    @Override
    public List<Long> getPartitionIds() {
        return baseTable.getPartitionIds();
    }

    public Map<Long, Pair<Long, Long>> getOutputUpdateMap() {
        return outputUpdateMap;
    }

    public OlapTableStreamUpdate getOutputUpdate() {
        return new OlapTableStreamUpdate(outputUpdate);
    }

    public Long getStreamDbId() {
        return stream.getDatabase().getId();
    }

    public Long getStreamId() {
        return stream.getId();
    }

    @Override
    public boolean hasDeleteSign() {
        return getDeleteSignColumn() != null;
    }

    @Override
    public boolean getEnableUniqueKeyMergeOnWrite() {
        return baseTable.getEnableUniqueKeyMergeOnWrite();
    }

    @Override
    public boolean isMorTable() {
        return baseTable.isMorTable();
    }

    @Override
    public Collection<Partition> getPartitions() {
        return baseTable.getPartitions();
    }

    @Override
    public List<Long> selectNonEmptyPartitionIds(Collection<Long> partitionIds) {
        List<Long> nonEmptyIds = Lists.newArrayListWithCapacity(partitionIds.size());
        for (Long partitionId : partitionIds) {
            if (stream.hasData(getPartition(partitionId))) {
                nonEmptyIds.add(partitionId);
            }
        }
        return nonEmptyIds;
    }

    public List<Long> filterHistoryPartitionIds(List<Long> partitionIds) {
        return partitionIds.stream()
                .filter(partitionId -> stream.hasHistoricalData(partitionId))
                .collect(ImmutableList.toImmutableList());
    }

    public List<Long> filterIncrementalPartitionIds(List<Long> partitionIds) {
        return partitionIds.stream()
                .filter(partitionId -> !stream.hasHistoricalData(partitionId)
                        && stream.hasData(getPartition(partitionId)))
                .collect(ImmutableList.toImmutableList());
    }

    public List<Long> filterConsumedPartitionIds(List<Long> partitionIds) {
        if (capturedUpdate) {
            Set<Long> capturedPartitionIds = outputUpdate.getNext().keySet();
            return partitionIds.stream()
                    .filter(capturedPartitionIds::contains)
                    .collect(ImmutableList.toImmutableList());
        }
        return partitionIds.stream()
                .filter(partitionId -> stream.hasConsumedData(partitionId))
                .collect(ImmutableList.toImmutableList());
    }

    public OlapTable getBaseTable() {
        return baseTable;
    }

    public BaseTableStream.StreamScanType getStreamScanType() {
        if (keysType == KeysType.DUP_KEYS) {
            return BaseTableStream.StreamScanType.APPEND_ONLY;
        }
        return stream.getStreamScanType();
    }

    public Map<Long, Pair<Long, Long>> getPartitionOffsets(List<Long> selectedPartitionIds) {
        if (capturedUpdate) {
            Set<Long> selectedPartitionIdSet = Sets.newHashSet(selectedPartitionIds);
            return outputUpdate.getNext().entrySet().stream()
                    .filter(entry -> selectedPartitionIdSet.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey,
                            entry -> Pair.of(entry.getValue(), getPartition(entry.getKey()).getTso())));
        }
        return outputUpdateMap.entrySet().stream()
                .filter(s -> selectedPartitionIds.contains(s.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    // get history partition offsets partitionId -> (null, historicalTimestampOffset)
    public Map<Long, Pair<Long, Long>> getHistoryPartitionOffsets(List<Long> selectedPartitionIds) {
        if (capturedUpdate) {
            Set<Long> selectedPartitionIdSet = Sets.newHashSet(selectedPartitionIds);
            return outputUpdate.getNext().entrySet().stream()
                    .filter(entry -> selectedPartitionIdSet.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> Pair.of(null, entry.getValue())));
        }
        return outputUpdateMap.entrySet().stream()
                .filter(s -> selectedPartitionIds.contains(s.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, s -> Pair.of(null, s.getValue().first)));
    }

    public List<Long> filterNormalSnapshotPartitionIds(List<Long> partitionIds) {
        if (capturedUpdate) {
            return partitionIds.stream()
                    .filter(partitionId -> outputUpdate.getNext().containsKey(partitionId)
                            && Objects.equals(outputUpdate.getNext().get(partitionId),
                                    getPartition(partitionId).getTso()))
                    .collect(Collectors.toList());
        }
        return partitionIds.stream()
                .filter(partitionId -> !stream.hasData(getPartition(partitionId)))
                .collect(Collectors.toList());
    }
}
