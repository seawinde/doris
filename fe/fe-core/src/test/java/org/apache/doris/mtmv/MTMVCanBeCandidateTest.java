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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.mtmv.MTMVRefreshEnum.BuildMode;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshTrigger;

import org.junit.Assert;
import org.junit.Test;

public class MTMVCanBeCandidateTest {

    private MTMV createMTMV(RefreshMethod method) {
        MTMV mtmv = new MTMV();
        MTMVRefreshTriggerInfo triggerInfo = new MTMVRefreshTriggerInfo(RefreshTrigger.SCHEDULE,
                new MTMVRefreshSchedule("ss", 2, IntervalUnit.SECOND));
        MTMVRefreshInfo refreshInfo = new MTMVRefreshInfo(BuildMode.IMMEDIATE, method, triggerInfo);
        mtmv.setRefreshInfo(refreshInfo);
        MTMVStatus status = new MTMVStatus();
        status.setState(MTMVState.NORMAL);
        status.setRefreshState(MTMVRefreshState.SUCCESS);
        mtmv.setStatus(status);
        return mtmv;
    }

    // TC-1-3: canBeCandidate() 对 INCREMENTAL MV 返回 false
    @Test
    public void testCanBeCandidateReturnsFalseForIncremental() {
        MTMV mtmv = createMTMV(RefreshMethod.INCREMENTAL);
        Assert.assertFalse(mtmv.canBeCandidate());
    }

    // TC-1-4: canBeCandidate() 对 COMPLETE MV 行为不变
    @Test
    public void testCanBeCandidateUnchangedForComplete() {
        MTMV mtmv = createMTMV(RefreshMethod.COMPLETE);
        Assert.assertTrue(mtmv.canBeCandidate());
    }

    // TC-1-6: AUTO MV 仍参与透明改写（与 INCREMENTAL 区分）
    @Test
    public void testAutoMVCanBeCandidate() {
        MTMV mtmv = createMTMV(RefreshMethod.AUTO);
        Assert.assertTrue(mtmv.canBeCandidate());
    }

    // TC-1-7: refreshInfo 为 null 时 canBeCandidate() 不 NPE，走原有逻辑
    @Test
    public void testCanBeCandidateNullRefreshInfoNoNPE() {
        MTMV mtmv = new MTMV();
        MTMVStatus status = new MTMVStatus();
        status.setState(MTMVState.NORMAL);
        status.setRefreshState(MTMVRefreshState.SUCCESS);
        mtmv.setStatus(status);
        // refreshInfo is null — should not NPE, should delegate to status
        Assert.assertTrue(mtmv.canBeCandidate());
    }
}
