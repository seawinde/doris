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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class IvmExcludedTriggerTableVersionChecker {

    public IvmExcludedTriggerTableVersionResult check(MTMV mtmv, MTMVRelation relation) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        Set<TableNameInfo> excludedTriggerTables = mtmv.getExcludedTriggerTables();
        if ((relation == null || relation.getBaseTablesOneLevelAndFromView() == null)
                && excludedTriggerTables != null && !excludedTriggerTables.isEmpty()) {
            return IvmExcludedTriggerTableVersionResult.dirty(Maps.newHashMap(),
                    "IVM excluded_trigger_tables requires COMPLETE refresh before INCREMENTAL refresh.");
        }
        Map<BaseTableInfo, Long> currentVersions = captureExcludedTriggerTableVersions(mtmv, relation);
        Map<BaseTableInfo, Long> baselineVersions = mtmv.getIvmInfo().getExcludedTriggerTableVersions();
        String currentSignature = excludedTriggerTablesSignature(excludedTriggerTables);
        String baselineSignature = mtmv.getIvmInfo().getExcludedTriggerTablesSignature();
        if (baselineSignature == null && excludedTriggerTables != null && !excludedTriggerTables.isEmpty()) {
            return IvmExcludedTriggerTableVersionResult.dirty(currentVersions,
                    "IVM excluded_trigger_tables requires COMPLETE refresh before INCREMENTAL refresh.");
        }
        if (baselineSignature != null && !StringUtils.equals(currentSignature, baselineSignature)) {
            return IvmExcludedTriggerTableVersionResult.dirty(currentVersions,
                    "IVM excluded_trigger_tables changed; "
                            + "COMPLETE refresh is required before INCREMENTAL refresh.");
        }
        if (MapUtils.isEmpty(currentVersions) && MapUtils.isEmpty(baselineVersions)) {
            return IvmExcludedTriggerTableVersionResult.clean(currentVersions);
        }
        if (MapUtils.isEmpty(baselineVersions)) {
            return IvmExcludedTriggerTableVersionResult.dirty(currentVersions,
                    "IVM excluded_trigger_tables requires COMPLETE refresh before INCREMENTAL refresh.");
        }
        if (!currentVersions.equals(baselineVersions)) {
            return IvmExcludedTriggerTableVersionResult.dirty(currentVersions,
                    "IVM excluded_trigger_tables changed or excluded table version changed; "
                            + "COMPLETE refresh is required before INCREMENTAL refresh.");
        }
        return IvmExcludedTriggerTableVersionResult.clean(currentVersions);
    }

    public Map<BaseTableInfo, Long> captureExcludedTriggerTableVersions(MTMV mtmv, MTMVRelation relation) {
        Objects.requireNonNull(mtmv, "mtmv can not be null");
        Map<BaseTableInfo, Long> versions = Maps.newHashMap();
        if (relation == null || relation.getBaseTablesOneLevelAndFromView() == null) {
            return versions;
        }
        Set<TableNameInfo> excludedTriggerTables = mtmv.getExcludedTriggerTables();
        if (excludedTriggerTables == null || excludedTriggerTables.isEmpty()) {
            return versions;
        }
        for (BaseTableInfo baseTableInfo : relation.getBaseTablesOneLevelAndFromView()) {
            TableIf table;
            try {
                table = MTMVUtil.getTable(baseTableInfo);
            } catch (AnalysisException e) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                        "Failed to resolve IVM base table: " + baseTableInfo, e);
            }
            if (!isExcludedTable(excludedTriggerTables, table)) {
                continue;
            }
            if (!(table instanceof OlapTable)) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                        "IVM excluded_trigger_tables only supports OLAP table: " + baseTableInfo);
            }
            versions.put(baseTableInfo, getVisibleVersion((OlapTable) table, baseTableInfo));
        }
        return versions;
    }

    public String excludedTriggerTablesSignature(Set<TableNameInfo> excludedTriggerTables) {
        if (excludedTriggerTables == null || excludedTriggerTables.isEmpty()) {
            return "";
        }
        return excludedTriggerTables.stream()
                .map(TableNameInfo::toString)
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.joining(","));
    }

    @VisibleForTesting
    long getVisibleVersion(OlapTable table, BaseTableInfo baseTableInfo) {
        try {
            return table.getVisibleVersion();
        } catch (Exception e) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "Failed to get visible version for IVM excluded table: " + baseTableInfo, e);
        }
    }

    private boolean isExcludedTable(Set<TableNameInfo> excludedTriggerTables, TableIf table) {
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo == null) {
            return false;
        }
        return MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, tableNameInfo);
    }
}
