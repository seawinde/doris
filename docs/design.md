# 1. 文档目的
本文基于当前 ivm-dev 分支代码，对 INCREMENTAL 异步物化视图相关问题做一次收敛评审，目标有三件事：
1. 检查原文档里已经给出的解法是否正确。
2. 给原文档里还没有解法的问题补上可执行方案。
本文以当前代码为准，重点核对以下路径：
- CreateMTMVInfo
- AlterMTMVRefreshInfo
- MTMVTask
- Env.getMTMVDdl
- MTMVPlanUtil.validateColumns
- IvmRefreshManager
- IvmDeltaExecutor
- IvmDeltaRewriter
- MTMV.canBeCandidate
相关公开文档链接：
- 异步物化视图概述
- CREATE ASYNC MATERIALIZED VIEW
- REFRESH MATERIALIZED VIEW

---
# 2. 核心语义约定

## 2.1 RefreshMethod（MV 类型 — CREATE 时指定，永久存储）

RefreshMethod 是 MV 的创建属性，存储在 `MTMVRefreshInfo.refreshMethod` 中，值为 `COMPLETE`、`AUTO`、`INCREMENTAL`。

- **COMPLETE**：每次刷新都全量重算所有分区。
- **AUTO**（默认值，不写 REFRESH 子句时的默认）：存储态永远保持 AUTO 不变。每次刷新时，系统根据 MV 的能力字段动态决定策略：
  - 检查 `MTMV.isIvm()`（即 `IvmInfo.enableIvm`）→ true → 走 INCREMENTAL（IVM delta）路径
  - 检查 `MTMVPartitionInfo` → 有分区映射 → 走分区级刷新路径（只刷过期分区）
  - 都没有 → 退化为 COMPLETE（全量刷新）
- **INCREMENTAL**：显式声明此 MV 为 IVM MV，物理模型为 UNIQUE_KEYS + MOW + 隐藏列 `__DORIS_IVM_ROW_ID_COL__`。

> **关键区别**：RefreshMethod.AUTO 不会在创建后被替换为 INCREMENTAL 或 COMPLETE。AUTO 始终存储为 AUTO，刷新策略在运行时通过 `IvmInfo` 和 `MTMVPartitionInfo` 动态决定。

## 2.2 RefreshMode（单次刷新指令 — REFRESH 命令时指定，瞬时）

RefreshMode 是手动 `REFRESH MATERIALIZED VIEW` 命令的参数，值为 `AUTO`、`COMPLETE`、`INCREMENTAL`、`PARTITIONS`。

- CREATE MATERIALIZED VIEW ... REFRESH INCREMENTAL：
  - 定义的是该 MV 的类型，表示默认刷新路径优先尝试 IVM。
  - 这不等于以后每次刷新都必须严格走 IVM。
- REFRESH MATERIALIZED VIEW ... INCREMENTAL：
  - 定义的是一次显式的手工 refresh mode（strict mode）。
  - 这次刷新必须走 IVM。
  - precheck / rewrite / execute 任一不满足都直接失败，不允许静默 fallback。
- REFRESH MATERIALIZED VIEW ... PARTITIONS：
  - 定义的是一次显式的脏分区增量刷新 mode。
  - 只对具备 INCREMENTAL 能力的 MV 开放（通过 IvmInfo 判断）。
  - 系统先识别脏分区，再对这些脏分区执行增量刷新。
  - 这不是旧的 partitionSpec 手工点名分区语义。
- REFRESH MATERIALIZED VIEW ... AUTO：
  - 优先尝试 IVM。
  - 失败允许回退到现有分区刷新或全量刷新路径。
- REFRESH MATERIALIZED VIEW ... COMPLETE：
  - 强制全量刷新，无论 MV 类型如何。

> **必须指定 RefreshMode**：`REFRESH MATERIALIZED VIEW mv_name` 不写模式关键字是语法错误，必须指定 AUTO / COMPLETE / INCREMENTAL / PARTITIONS 之一。

## 2.3 RefreshMethod 与 RefreshMode 的关系矩阵

| MV RefreshMethod ↓ / RefreshMode → | AUTO | COMPLETE | INCREMENTAL | PARTITIONS | PARTITION(p1) 旧语法 |
|-----|------|----------|-------------|------------|---------------------|
| COMPLETE | ✅ | ✅ | ❌ | ❌ | ✅ |
| AUTO（isIvm=true） | ✅ | ✅ | ✅ | ✅ | ❌ |
| AUTO（isIvm=false） | ✅ | ✅ | ❌ | ❌ | ✅ |
| INCREMENTAL | ✅ | ✅ | ✅ | ✅ | ❌ |

> 判断"是否具备 INCREMENTAL 能力"的依据是 `MTMV.isIvm()`（底层为 `IvmInfo.enableIvm` 持久化标志），而非 `RefreshMethod == INCREMENTAL`。AUTO MV 如果创建时系统判定其可支持增量刷新，`enableIvm` 也会为 true。

## 2.4 刷新时 MV 能力判断

每次刷新时，系统不依赖 RefreshMethod 枚举值选择路径，而是检查 MV 上的能力字段：

| 字段 | 含义 | 为空时退化策略 |
|------|------|---------------|
| `IvmInfo.enableIvm` | true 则该 MV 具备 IVM 增量刷新能力，通过 `MTMV.isIvm()` 访问 | 不走 INCREMENTAL 路径 |
| `MTMVPartitionInfo` | 存在且有分区映射则具备分区级刷新能力 | 不走分区刷新，退化为 COMPLETE |

## 2.5 首刷 bootstrap

- 对于 INCREMENTAL MV，首次刷新始终是全量刷新。
- BUILD IMMEDIATE / DEFERRED 只影响这次全量首刷是立刻触发还是以后触发，不影响它必须先全量。

---
# 3. 总结结论
## 3.1 当前代码里已经基本解决的点
- INCREMENTAL MV 的底层物理模型已切到 UNIQUE_KEYS + MOW。
- row-id 注入链路已经打通。
- sink 对齐问题已经有专门处理，BindSink 和 UpdateMvByPartitionCommand 已能识别 IVM hidden column。
- MTMV 刷新期间关闭 DML MV rewrite 的修复已经在代码里。
- partitionSnapshots == null 不会直接触发 NPE，因为 `MTMVRefreshSnapshot.updateSnapshots()` 对 null 输入是空操作。IVM 成功后走与普通 MV 相同的回调路径（refresh state + partition snapshot + rewrite cache），但缺少 IVM progress 元数据的额外更新（见 P1-1）。
## 3.2 当前仍然是 P0 阻塞项的点
- CREATE 侧的 INCREMENTAL 组合约束没有收紧。
- ~~REFRESH MATERIALIZED VIEW 还不能显式写 INCREMENTAL~~（已在 PR-1 中解决）。~~RefreshMTMVInfo 仍用 isComplete 布尔值~~（已替换为 RefreshMode 枚举）。
- ALTER MATERIALIZED VIEW ... REFRESH INCREMENTAL 仍未禁止。
- SHOW CREATE MATERIALIZED VIEW 仍会输出非法或泄露内部实现的 DDL。
- validateColumns() 仍按 DUP_KEYS 校验 INCREMENTAL MV。
- INCREMENTAL MV 参与透明改写候选集（正确行为，已在 PR-9 中补充测试验证）。
- AUTO 刷新方式的物化视图缺少对 binlog 开关状态的提示和文档说明。
- `validateRefreshModeCompat()` 当前基于 `MTMV.isIvm()` 判断 MV 是否具备 INCREMENTAL 能力（底层为 `IvmInfo.enableIvm` 持久化标志）。
注：capability gate / checkStreamSupport、delta rewrite、watermark / stream progress 由其他文档覆盖，不在本文档解决。
## 3.3 原文里需要修正的几个判断
- AUTO 不能等价理解为"增量刷新"。RefreshMethod.AUTO 的含义是"系统动态决定"，每次刷新时根据 IvmInfo / MTMVPartitionInfo 选择最优路径（优先 INCREMENTAL → 分区刷新 → COMPLETE）。RefreshMethod.AUTO 永远存储为 AUTO，不会被替换为 INCREMENTAL。
- 判断 MV 是否具备 INCREMENTAL 能力，应使用 `MTMV.isIvm()`（底层为 `IvmInfo.enableIvm`），而非 `RefreshMethod == INCREMENTAL`。AUTO MV 如果创建时 `enableIvm = true`，也受 INCREMENTAL 相关约束（如禁止旧 partitionSpec 语法）。
- REFRESH MATERIALIZED VIEW 需要按物化视图类型区分语义，但要把"MV 类型"和"本次手工刷新 mode"分开。
- INCREMENTAL + PARTITION BY 不应在 analyze 阶段一刀切禁止。当前更准确的语义是：允许指定 PARTITION BY；但如果当前定义不能支持分区增量刷新，创建阶段就直接报错。
- BUILD IMMEDIATE 不应作为 INCREMENTAL 的建表硬限制。
- 不再需要"增量刷新重试时间"或"冷却窗口"等抑制机制。
- 降级兼容不应该把未知 refresh method 静默回退为 COMPLETE。
- IVM 物理模型变化（UNIQUE_KEYS + MOW + row-id）不应在 CREATE / SHOW CREATE 层面暴露给用户，但 sink 对齐已经不再是未解决 blocker。

---
# 4. 短期明确不支持列表
在 P0 完成前，建议直接把以下组合在 analyze 阶段拒绝掉：
- ALTER MATERIALIZED VIEW ... REFRESH INCREMENTAL（从非 INCREMENTAL 改为 INCREMENTAL）
- 从 INCREMENTAL 改回 AUTO / COMPLETE
- INCREMENTAL + 用户自定义 KEY（包括 UNIQUE KEY 和 DUPLICATE KEY，增量物化视图不允许指定任何表模型和 KEY）
- INCREMENTAL + AGG_KEYS / 非 MOW UNIQUE_KEYS 基表（除非该基表在 excluded_trigger_tables 中）
- 不允许把已有 async MV 的物理模型从 DUP_KEYS 改成 UNIQUE_KEYS + MOW，不允许原地把普通 async MV 升级成 IVM MV，只能删建
注意：
- BUILD DEFERRED、PARTITION BY、MTMV 属性不再建议做"一刀切禁止"，这些能力应保留。
- INCREMENTAL MV 参与透明改写，与 COMPLETE/AUTO MV 行为一致。
- 上述约束中"INCREMENTAL MV"的判断依据是 `MTMV.isIvm()`（底层为 `IvmInfo.enableIvm` 持久化标志），而非 `RefreshMethod == INCREMENTAL`。AUTO MV 如果创建时系统判定其可支持增量刷新（`enableIvm = true`），也应受相同约束。

---
# 5. 逐项评审与实施方案
## 5.2 P0-1：INCREMENTAL MV 参与透明改写
### 问题描述
在增量刷新过程中，系统能够感知分区级别的数据新鲜度，因此可以知道物化视图的哪些分区是失效的。INCREMENTAL MV 参与透明改写，与 COMPLETE/AUTO MV 行为一致。
### 当前代码事实
- `MTMV.canBeCandidate()` 不区分 `RefreshMethod`，仅依赖 `MTMVStatus`（state == NORMAL && refreshState != INIT）判定候选资格。
- `MTMV.addTaskResult()` 在 INCREMENTAL 成功时正常构建 rewrite cache，供透明改写使用。
- 以上行为正确，无需修改。
### 修正方案
修改位置：无代码修改，仅补充测试用例。
主方案：
- 保持 `MTMV.canBeCandidate()` 现有逻辑不变，INCREMENTAL MV 与 COMPLETE/AUTO MV 行为一致
- 保持 `MTMV.addTaskResult()` 现有逻辑不变，INCREMENTAL MV 刷新成功后正常构建 `MTMVCache`
- 补充 FE 单元测试 + 回归测试验证透明改写在 INCREMENTAL MV 上的正确性

### 测试用例

A. FE 单元测试
测试文件：MTMVCanBeCandidateTest.java（新建）
```java
// TC-1-1: INCREMENTAL MV 在 NORMAL+SUCCESS 状态下参与透明改写
@Test public void testIncrementalMVCanBeCandidateWhenNormalAndSuccess() {
    MTMV mtmv = createMTMVWithRefreshMethod(RefreshMethod.INCREMENTAL, MTMVState.NORMAL, MTMVRefreshState.SUCCESS);
    Assert.assertTrue(mtmv.canBeCandidate());
}
// TC-1-2: INCREMENTAL MV 在 NORMAL+FAIL 状态下仍参与透明改写（部分分区可能已同步）
@Test public void testIncrementalMVCanBeCandidateWhenNormalAndFail() {
    MTMV mtmv = createMTMVWithRefreshMethod(RefreshMethod.INCREMENTAL, MTMVState.NORMAL, MTMVRefreshState.FAIL);
    Assert.assertTrue(mtmv.canBeCandidate());
}
// TC-1-3: INCREMENTAL MV 在 INIT 状态下不参与透明改写（与其他类型一致）
@Test public void testIncrementalMVNotCandidateWhenInit() {
    MTMV mtmv = createMTMVWithRefreshMethod(RefreshMethod.INCREMENTAL, MTMVState.INIT, MTMVRefreshState.INIT);
    Assert.assertFalse(mtmv.canBeCandidate());
}
// TC-1-4: 所有 RefreshMethod（COMPLETE/AUTO/INCREMENTAL）的候选行为一致
@Test public void testAllRefreshMethodsBehaveIdenticallyForCandidacy() {
    for (RefreshMethod method : RefreshMethod.values()) {
        MTMV mtmv = createMTMVWithRefreshMethod(method, MTMVState.NORMAL, MTMVRefreshState.SUCCESS);
        Assert.assertTrue(mtmv.canBeCandidate());
    }
}

```
B. 回归测试
测试文件：regression-test/suites/mtmv_p0/test_ivm_transparent_rewrite.groovy（新建）

核心验证思路：对 INCREMENTAL MV 做与 COMPLETE/AUTO MV 相同的透明改写验证，确保在创建、首次刷新、增量刷新、分区脏数据等场景下改写行为一致且数据正确。

每个 TC 的验证方法：
- `mv_rewrite_success(query, mvName)` — explain 确认查询走 MV 改写
- `mv_rewrite_fail(query, mvName)` — explain 确认未走 MV 改写
- `order_qt_xxx` — 执行查询并与 .out 文件比对结果（自动生成）
- 关闭改写 `SET enable_materialized_view_rewrite=false` 直查基表，对比开启改写后结果一致

--- 非分区场景 ---

TC-1-5: 简单 SELECT * 的非分区 INCREMENTAL MV 透明改写
验证目标：INCREMENTAL MV 首次全量刷新后，简单 SELECT * 能被透明改写到 MV。
```groovy
// 基表
sql "DROP TABLE IF EXISTS t_ivm_rw_base"
sql """CREATE TABLE t_ivm_rw_base (k1 INT, v1 INT, v2 VARCHAR(32))
       DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1"""
sql "INSERT INTO t_ivm_rw_base VALUES (1,10,'a'),(2,20,'b'),(3,30,'c')"

// INCREMENTAL MV
sql "DROP MATERIALIZED VIEW IF EXISTS mv_ivm_rw"
sql """CREATE MATERIALIZED VIEW mv_ivm_rw BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
       AS SELECT * FROM t_ivm_rw_base"""
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw")

// 验证改写
def query = "SELECT k1, v1, v2 FROM t_ivm_rw_base"
mv_rewrite_success(query, "mv_ivm_rw")
order_qt_basic_rewrite "${query}"
// 预期结果: 1,10,a | 2,20,b | 3,30,c
```

TC-1-6: 增量刷新后透明改写数据正确
验证目标：基表 INSERT 新数据后做增量刷新，MV 改写仍命中且数据包含新增行。
```groovy
// 续 TC-1-5
sql "INSERT INTO t_ivm_rw_base VALUES (4,40,'d'),(5,50,'e')"
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw")

mv_rewrite_success(query, "mv_ivm_rw")
order_qt_after_incr_refresh "${query}"
// 预期结果: 1,10,a | 2,20,b | 3,30,c | 4,40,d | 5,50,e
```

TC-1-7: 列投影（列裁剪）的 INCREMENTAL MV 改写
验证目标：查询只投影部分列时，仍能被改写到包含全部列的 MV。
```groovy
// 续 TC-1-5/6
def proj_query = "SELECT k1, v1 FROM t_ivm_rw_base"
mv_rewrite_success(proj_query, "mv_ivm_rw")
order_qt_projection "${proj_query}"
// 预期结果: 1,10 | 2,20 | 3,30 | 4,40 | 5,50
```

TC-1-8: INIT 状态 INCREMENTAL MV 不参与透明改写（负向验证）
验证目标：BUILD DEFERRED 但未刷新的 MV，状态为 INIT，不应被选为改写候选。
```groovy
sql "DROP TABLE IF EXISTS t_ivm_rw_init"
sql """CREATE TABLE t_ivm_rw_init (k1 INT, v1 INT)
       DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1"""
sql "INSERT INTO t_ivm_rw_init VALUES (1,10)"

sql "DROP MATERIALIZED VIEW IF EXISTS mv_ivm_rw_init"
sql """CREATE MATERIALIZED VIEW mv_ivm_rw_init BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
       AS SELECT * FROM t_ivm_rw_init"""
// 不触发刷新 → 状态为 INIT
def init_query = "SELECT k1, v1 FROM t_ivm_rw_init"
mv_rewrite_fail(init_query, "mv_ivm_rw_init")
```

TC-1-9: 非分区 INCREMENTAL MV 基表 DML 后未刷新，改写行为
验证目标：基表变脏但 MV 未刷新时，非分区 MV 整体失效，grace_period=0 时不改写。查询结果与直查基表一致。
```groovy
// 续 TC-1-5/6
sql "INSERT INTO t_ivm_rw_base VALUES (6,60,'f')"
// 不刷新 MV → MV 数据过期
sql "SET enable_materialized_view_rewrite=false"
order_qt_dirty_before "${query}"  // 直查基表: 6 行
sql "SET enable_materialized_view_rewrite=true"
// 非分区 MV 整体变脏 → 取决于 grace_period 配置
// grace_period=0 时不改写，查询仍正确（走基表）
order_qt_dirty_after "${query}"
// 预期: dirty_before 和 dirty_after 结果一致
```

--- 分区场景 ---

TC-1-10: 分区 INCREMENTAL MV 全部分区新鲜时透明改写
验证目标：分区 MV 所有分区都已刷新且新鲜，全表查询能被完全改写到 MV。
```groovy
sql "DROP TABLE IF EXISTS t_ivm_rw_part"
sql """CREATE TABLE t_ivm_rw_part (k1 INT, v1 INT, pt DATE)
       DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
       PARTITION BY RANGE(pt)(FROM ('2024-01-01') TO ('2024-01-10') INTERVAL 1 DAY)"""
sql "INSERT INTO t_ivm_rw_part VALUES (1,10,'2024-01-01'),(2,20,'2024-01-02'),(3,30,'2024-01-03')"

sql "DROP MATERIALIZED VIEW IF EXISTS mv_ivm_rw_part"
sql """CREATE MATERIALIZED VIEW mv_ivm_rw_part BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
       PARTITION BY(pt) AS SELECT * FROM t_ivm_rw_part"""
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_part AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_part")

def query_all = "SELECT k1, v1, pt FROM t_ivm_rw_part"
mv_rewrite_success(query_all, "mv_ivm_rw_part")
order_qt_part_all_fresh "${query_all}"
// 预期结果: 1,10,2024-01-01 | 2,20,2024-01-02 | 3,30,2024-01-03
```

TC-1-11: 分区 INCREMENTAL MV——部分分区脏时 union rewrite
验证目标：只弄脏一个分区后，查干净分区走 MV，全表查询走 union rewrite（新鲜分区走 MV + 脏分区走基表），结果正确。
```groovy
// 续 TC-1-10
sql "INSERT INTO t_ivm_rw_part VALUES (4,40,'2024-01-01')"  // 弄脏 2024-01-01 分区
// 等待感知到脏分区
waitingPartitionIsExpected("mv_ivm_rw_part", "p_20240101_20240102", false)

// 只查干净分区 → 改写命中
def query_clean = "SELECT k1, v1, pt FROM t_ivm_rw_part WHERE pt >= '2024-01-02'"
mv_rewrite_success(query_clean, "mv_ivm_rw_part")

// 全表查询 → union rewrite
mv_rewrite_success(query_all, "mv_ivm_rw_part")
sql "SET enable_materialized_view_rewrite=false"
order_qt_part_union_before "${query_all}"
sql "SET enable_materialized_view_rewrite=true"
order_qt_part_union_after "${query_all}"
// 预期: before 和 after 结果一致（包含 4 行）
```

TC-1-12: 分区 INCREMENTAL MV——增量刷新脏分区后恢复全量改写
验证目标：增量刷新修复脏分区后，全表查询重新走完整 MV 改写，数据正确。
```groovy
// 续 TC-1-11
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_part AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_part")

mv_rewrite_success(query_all, "mv_ivm_rw_part")
order_qt_part_after_incr "${query_all}"
// 预期结果: 1,10,... | 2,20,... | 3,30,... | 4,40,...
```

TC-1-13: 分区 INCREMENTAL MV——基表新增分区后改写行为
验证目标：基表新增分区数据后，旧分区仍可改写，新分区走基表（union rewrite），增量刷新后全部可改写。
```groovy
// 续 TC-1-12
sql "INSERT INTO t_ivm_rw_part VALUES (5,50,'2024-01-05')"  // 新增分区
// 旧分区干净 → 查旧分区仍改写
mv_rewrite_success("SELECT k1,v1,pt FROM t_ivm_rw_part WHERE pt='2024-01-02'", "mv_ivm_rw_part")
// 全表 → union rewrite
mv_rewrite_success(query_all, "mv_ivm_rw_part")
sql "SET enable_materialized_view_rewrite=false"
order_qt_new_part_before "${query_all}"
sql "SET enable_materialized_view_rewrite=true"
order_qt_new_part_after "${query_all}"
// 增量刷新
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_part AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_part")
order_qt_new_part_final "${query_all}"
// 预期: 5 行全部正确
```

--- 聚合场景 ---

TC-1-14: 聚合 INCREMENTAL MV 透明改写 + 增量刷新数据正确
验证目标：聚合查询（SUM/COUNT）能被改写到聚合 MV，增量刷新后聚合结果正确累加。
```groovy
sql "DROP TABLE IF EXISTS t_ivm_rw_agg"
sql """CREATE TABLE t_ivm_rw_agg (k1 INT, v1 DECIMAL(15,2), cat VARCHAR(16))
       DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1"""
sql "INSERT INTO t_ivm_rw_agg VALUES (1,10.5,'a'),(1,20.5,'a'),(2,30.0,'b'),(3,40.0,'c')"

sql "DROP MATERIALIZED VIEW IF EXISTS mv_ivm_rw_agg"
sql """CREATE MATERIALIZED VIEW mv_ivm_rw_agg BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
       AS SELECT cat, SUM(v1) as total, COUNT(*) as cnt FROM t_ivm_rw_agg GROUP BY cat"""
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_agg AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_agg")

def agg_query = "SELECT cat, SUM(v1), COUNT(*) FROM t_ivm_rw_agg GROUP BY cat"
mv_rewrite_success(agg_query, "mv_ivm_rw_agg")
order_qt_agg_rewrite "${agg_query}"
// 预期: a,31.00,2 | b,30.00,1 | c,40.00,1

// 增量追加
sql "INSERT INTO t_ivm_rw_agg VALUES (1,5.5,'a'),(4,50.0,'d')"
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_agg AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_agg")
mv_rewrite_success(agg_query, "mv_ivm_rw_agg")
order_qt_agg_after_incr "${agg_query}"
// 预期: a,36.50,3 | b,30.00,1 | c,40.00,1 | d,50.00,1
```

--- JOIN 场景 ---

TC-1-15: 多表 JOIN 的 INCREMENTAL MV 透明改写
验证目标：多表 JOIN 查询能被改写到 JOIN MV，增量刷新后 JOIN 结果包含新数据。
```groovy
sql "DROP TABLE IF EXISTS t_ivm_orders"
sql """CREATE TABLE t_ivm_orders (order_id INT, cust_id INT, amount DECIMAL(10,2))
       DUPLICATE KEY(order_id) DISTRIBUTED BY HASH(order_id) BUCKETS 1"""
sql "DROP TABLE IF EXISTS t_ivm_customers"
sql """CREATE TABLE t_ivm_customers (cust_id INT, name VARCHAR(32))
       DUPLICATE KEY(cust_id) DISTRIBUTED BY HASH(cust_id) BUCKETS 1"""
sql "INSERT INTO t_ivm_orders VALUES (1,1,100),(2,2,200),(3,1,150)"
sql "INSERT INTO t_ivm_customers VALUES (1,'Alice'),(2,'Bob')"

sql "DROP MATERIALIZED VIEW IF EXISTS mv_ivm_rw_join"
sql """CREATE MATERIALIZED VIEW mv_ivm_rw_join BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
       AS SELECT o.order_id, c.name, o.amount
       FROM t_ivm_orders o INNER JOIN t_ivm_customers c ON o.cust_id = c.cust_id"""
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_join AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_join")

def join_query = """SELECT o.order_id, c.name, o.amount
    FROM t_ivm_orders o INNER JOIN t_ivm_customers c ON o.cust_id = c.cust_id"""
mv_rewrite_success(join_query, "mv_ivm_rw_join")
order_qt_join_rewrite "${join_query}"
// 预期: 1,Alice,100 | 2,Bob,200 | 3,Alice,150

// 增量追加
sql "INSERT INTO t_ivm_orders VALUES (4,2,300)"
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_join AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_join")
mv_rewrite_success(join_query, "mv_ivm_rw_join")
order_qt_join_after_incr "${join_query}"
// 预期: 1,Alice,100 | 2,Bob,200 | 3,Alice,150 | 4,Bob,300
```

--- 多次增量刷新场景 ---

TC-1-16: 多次增量刷新的累积正确性
验证目标：连续多次 INSERT + 增量刷新后，MV 数据累积正确，每轮刷新后改写结果都正确。
```groovy
sql "DROP TABLE IF EXISTS t_ivm_rw_multi"
sql """CREATE TABLE t_ivm_rw_multi (k1 INT, v1 INT)
       DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1"""
sql "INSERT INTO t_ivm_rw_multi VALUES (1,10),(2,20)"

sql "DROP MATERIALIZED VIEW IF EXISTS mv_ivm_rw_multi"
sql """CREATE MATERIALIZED VIEW mv_ivm_rw_multi BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
       AS SELECT * FROM t_ivm_rw_multi"""
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_multi AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_multi")

def multi_query = "SELECT k1, v1 FROM t_ivm_rw_multi"
mv_rewrite_success(multi_query, "mv_ivm_rw_multi")
order_qt_multi_r0 "${multi_query}"    // 预期: 1,10 | 2,20

sql "INSERT INTO t_ivm_rw_multi VALUES (3,30)"
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_multi AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_multi")
order_qt_multi_r1 "${multi_query}"    // 预期: 1,10 | 2,20 | 3,30

sql "INSERT INTO t_ivm_rw_multi VALUES (4,40)"
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_multi AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_multi")
order_qt_multi_r2 "${multi_query}"    // 预期: 1,10 | 2,20 | 3,30 | 4,40

sql "INSERT INTO t_ivm_rw_multi VALUES (5,50)"
sql "REFRESH MATERIALIZED VIEW mv_ivm_rw_multi AUTO"
waitingMTMVTaskFinishedByMvName("mv_ivm_rw_multi")
order_qt_multi_r3 "${multi_query}"    // 预期: 1,10 | 2,20 | 3,30 | 4,40 | 5,50

// 最终一致性验证
sql "SET enable_materialized_view_rewrite=false"
order_qt_multi_base "${multi_query}"
sql "SET enable_materialized_view_rewrite=true"
order_qt_multi_mv "${multi_query}"
// 预期: multi_base 和 multi_mv 结果完全一致
```

--- 谓词补偿场景 ---

TC-1-17: 带 WHERE 条件的查询对 INCREMENTAL MV 的谓词补偿改写
验证目标：查询带 WHERE 过滤条件时，能对 SELECT * 的 MV 做谓词补偿改写，结果正确。
```groovy
// 复用 TC-1-5 的 mv_ivm_rw（含 5 行数据）
def filter_query = "SELECT k1, v1, v2 FROM t_ivm_rw_base WHERE v1 > 20"
mv_rewrite_success(filter_query, "mv_ivm_rw")
order_qt_predicate "${filter_query}"
// 预期: 3,30,c | 4,40,d | 5,50,e
```

### 结论
INCREMENTAL MV 参与透明改写，与 COMPLETE/AUTO MV 行为一致。现有 `canBeCandidate()` 和 `addTaskResult()` 逻辑已正确支持，无需代码修改。本 PR 通过 FE 单元测试锁定候选行为，通过回归测试验证端到端改写正确性。

---
## 5.3 P0-2：细化 CREATE MTMV 的 INCREMENTAL 组合校验
### 问题描述
`CreateMTMVInfo.analyze()` 当前只禁止了 INCREMENTAL MV 显式指定 key，没有禁止 AGG_KEYS / 非 MOW UNIQUE_KEYS 基表。INCREMENTAL + PARTITION BY 允许使用，但如果当前定义不能支持分区增量刷新，创建阶段直接报错。所有 MTMV 属性对 INCREMENTAL MV 与之前的逻辑保持一致（属性矩阵详见 P0-10）。
基表模型校验规则：
- 参与增量刷新的基表必须是 UNIQUE_KEYS + MOW 或 DUP_KEYS
- AGG_KEYS 和非 MOW 的 UNIQUE_KEYS 基表不支持增量刷新
- 如果 AGG_KEYS / 非 MOW UNIQUE_KEYS 基表在 excluded_trigger_tables 中，则该表不参与增量刷新，跳过模型校验，允许创建
### 当前代码事实
- `CreateMTMVInfo.analyze()` 只禁止了 INCREMENTAL MV 显式指定 key，没有禁止 BUILD DEFERRED、ON COMMIT、PARTITION BY、MTMV 属性，也没有拒绝 AGG_KEYS / 非 MOW UNIQUE_KEYS 基表。
### 修正方案
### 修改位置：
- fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/CreateMTMVInfo.java
- fe/fe-core/src/main/java/org/apache/doris/mtmv/MTMVPlanUtil.java
必须落地的限制：
- 禁止用户显式指定 key
- 参与增量刷新的基表必须是 UNIQUE_KEYS + MOW 或 DUP_KEYS
- AGG_KEYS / 非 MOW UNIQUE_KEYS 基表不允许，除非在 excluded_trigger_tables 中
必须保留的现有能力：
- BUILD IMMEDIATE / DEFERRED
- PARTITION BY
- MTMV 属性透传
推荐实现顺序：
1. analyzeProperties() 后先做用户显式 key 校验
2. analyzeQuery() 后用 relation.getBaseTablesOneLevelAndFromView() 校验所有基表模型
3. setTableInformation() 只在所有校验通过后再执行
关键代码（在 analyze() 方法中 analyzeQuery() 之后添加）：
```java
if (isEnableIvm()) {
    validateIncrementalBaseTableModels(ctx);
}

private void validateIncrementalBaseTableModels(ConnectContext ctx) {
    Set<String> excludedTables = getExcludedTriggerTables(); // 从 MTMV 属性中获取
    for (BaseTableInfo baseTableInfo : relation.getBaseTablesOneLevelAndFromView()) {
        TableIf table = ...; // 查表
        if (excludedTables.contains(table.getName())) {
            continue; // 在 excluded_trigger_tables 中的表不参与增量刷新，跳过校验
        }
        if (table instanceof OlapTable) {
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getKeysType() != KeysType.UNIQUE_KEYS
                    && olapTable.getKeysType() != KeysType.DUP_KEYS) {
                // AGG_KEYS 或非 MOW UNIQUE_KEYS
                throw new AnalysisException(
                    "INCREMENTAL materialized view requires base tables to be "
                    + "UNIQUE_KEYS with Merge-On-Write or DUP_KEYS. Table '"
                    + olapTable.getName() + "' is " + olapTable.getKeysType()
                    + ". If this table does not participate in incremental refresh, "
                    + "add it to 'excluded_trigger_tables'.");
            }
            if (olapTable.getKeysType() == KeysType.UNIQUE_KEYS
                    && !olapTable.getEnableUniqueKeyMergeOnWrite()) {
                throw new AnalysisException(
                    "INCREMENTAL materialized view requires UNIQUE_KEYS base tables "
                    + "to enable Merge-On-Write. Table '"
                    + olapTable.getName() + "' has MOW disabled."
                    + " If this table does not participate in incremental refresh, "
                    + "add it to 'excluded_trigger_tables'.");
            }
        } else {
            throw new AnalysisException(
                "INCREMENTAL materialized view only supports OlapTable base tables. "
                + "Table '" + table.getName() + "' is " + table.getType());
        }
    }
}
```
### 测试用例
测试文件：CreateMTMVCommandTest.java（追加）
```java
// TC-2-1: INCREMENTAL MV 基表是 DUP_KEYS 应成功
@Test
public void testCreateIncrementalMVAcceptsDupKeysBaseTable() throws Exception { }
// TC-2-2: INCREMENTAL MV 基表是 UNIQUE_KEYS + MOW 应成功
@Test
public void testCreateIncrementalMVAcceptsMOWBaseTable() throws Exception { }
// TC-2-3: INCREMENTAL MV 不允许指定 UNIQUE KEY
@Test
public void testCreateIncrementalMVRejectsUniqueKey() throws Exception { }
// TC-2-12: INCREMENTAL MV 不允许指定 DUPLICATE KEY
@Test
public void testCreateIncrementalMVRejectsDuplicateKey() throws Exception { }
// TC-2-4: INCREMENTAL MV 允许 BUILD DEFERRED
@Test
public void testCreateIncrementalMVAllowsBuildDeferred() throws Exception { }
// TC-2-5: INCREMENTAL MV 在满足分区增量刷新校验时允许 PARTITION BY
@Test
public void testCreateIncrementalMVAllowsPartitionByWhenSupported() throws Exception { }
// TC-2-6: 指定 PARTITION BY 但不能分区增量刷新时，创建阶段应报错
@Test
public void testCreateIncrementalMVRejectsUnsupportedPartitionIncremental() throws Exception { }
// TC-2-7: INCREMENTAL MV 基表包含 AGG_KEYS 表应报错
@Test
public void testCreateIncrementalMVRejectsAggKeysBaseTable() throws Exception { }
// TC-2-8: 基表是 UNIQUE_KEYS 但未开 MOW → 应报错
@Test
public void testCreateIncrementalMVRejectsUniqueKeyWithoutMOW() throws Exception { }
// TC-2-9: 基表是非 OlapTable（外部表）→ 应报错
@Test
public void testCreateIncrementalMVRejectsNonOlapBaseTable() { }
// TC-2-10: 基表是 AGG_KEYS 且不在 excluded_trigger_tables 中 → 应报错
@Test
public void testCreateIncrementalMVRejectsAggKeysNotInExcluded() throws Exception { }
// TC-2-11: 基表是 AGG_KEYS 但在 excluded_trigger_tables 中 → 应成功创建
@Test
public void testCreateIncrementalMVAllowsAggKeysInExcluded() throws Exception { }
```
### 结论
收紧 CREATE 侧约束：参与增量刷新的基表必须是 UNIQUE_KEYS + MOW 或 DUP_KEYS；AGG_KEYS / 非 MOW UNIQUE_KEYS 基表不允许，除非在 excluded_trigger_tables 中。保留 BUILD DEFERRED、PARTITION BY、属性透传。

---
## 5.4 P0-3：禁止通过 ALTER 切换到或切出 INCREMENTAL
### 问题描述
当前 `AlterMTMVRefreshInfo.analyze()` 只做 `refreshInfo.validate()`，没有新旧 refresh method 兼容性校验。可以通过 ALTER 在普通 MV 和 INCREMENTAL MV 之间任意切换，但这两者物理模型不同（DUP_KEYS vs UNIQUE_KEYS+MOW），原地切换会导致不一致。
### 当前代码事实
- `AlterMTMVRefreshInfo.analyze()` 当前只做 `refreshInfo.validate()`，没有新旧 refresh method 兼容性校验。
### 修正方案
### 修改位置：
- fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/AlterMTMVRefreshInfo.java
在 analyze() 中显式查出当前 MTMV，比较 old/new refresh method，双向禁止。
关键代码：
```java
public void analyze(ConnectContext ctx) throws AnalysisException {
    super.analyze(ctx);
    refreshInfo.validate();
    try {
        Database db = Env.getCurrentInternalCatalog()
                .getDbOrDdlException(getMvName().getDb());
        MTMV mtmv = (MTMV) db.getTableOrMetaException(
                getMvName().getTbl(), TableIf.TableType.MATERIALIZED_VIEW);
        RefreshMethod oldMethod = mtmv.getRefreshInfo().getRefreshMethod();
        RefreshMethod newMethod = refreshInfo.getRefreshMethod();
        if (newMethod != null) {
            if (newMethod == RefreshMethod.INCREMENTAL
                    && oldMethod != RefreshMethod.INCREMENTAL) {
                throw new AnalysisException(
                    "Cannot ALTER refresh method to INCREMENTAL. "
                    + "Please recreate the MV.");
            }
            if (newMethod != RefreshMethod.INCREMENTAL
                    && oldMethod == RefreshMethod.INCREMENTAL) {
                throw new AnalysisException(
                    "Cannot ALTER refresh method from INCREMENTAL to "
                    + newMethod + ". Please recreate the MV.");
            }
        }
    } catch (org.apache.doris.common.AnalysisException
            | org.apache.doris.common.MetaNotFoundException
            | org.apache.doris.common.DdlException e) {
        throw new AnalysisException(e.getMessage());
    }
}
```
### 测试用例
测试文件：AlterMTMVTest.java（追加）
```java
// TC-3-1: 从 COMPLETE 改为 INCREMENTAL 应报错
@Test
public void testAlterFromCompleteToIncrementalRejected() throws Exception { }
// TC-3-2: 从 INCREMENTAL 改为 COMPLETE 应报错
@Test
public void testAlterFromIncrementalToCompleteRejected() throws Exception { }
// TC-3-3: 从 INCREMENTAL 改为 AUTO 应报错
@Test
public void testAlterFromIncrementalToAutoRejected() throws Exception { }
// TC-3-4: COMPLETE 改为 AUTO 应正常（原有行为不变）
@Test
public void testAlterFromCompleteToAutoAllowed() throws Exception { }
// TC-3-5: 从 AUTO 切换到 INCREMENTAL 也应被禁止
@Test
public void testAlterFromAutoToIncrementalRejected() throws Exception { }
```
### 结论
原解法方向正确，但示例代码要修正成"显式查 MTMV"，不能直接调用不存在的 getMTMV() 方法。

---
## 5.5 P0-4：修复 SHOW CREATE MATERIALIZED VIEW
### 问题描述
`Env.getMTMVDdl()` 仍通过物理 schema 反推 key 子句。对 INCREMENTAL MV，SHOW CREATE 会暴露 UNIQUE KEY(...)、hidden row-id 列等内部实现细节，生成的 DDL 不可重放。
### 当前代码事实
- `Env.getMTMVDdl()` 仍通过物理 schema 反推 key 子句。
- addColNameAndComment() 在 show_hidden_columns=true 时会把 row-id 列带出来。
### 修正方案
### 修改位置：
- fe/fe-core/src/main/java/org/apache/doris/catalog/Env.java
对 INCREMENTAL MV，SHOW CREATE 需要输出逻辑 DDL，而不是物理 DDL：
1. 跳过 UNIQUE KEY(...) 输出
2. 强制过滤 hidden row-id 列
3. 不输出 UNIQUE_KEYS + MOW 物理细节
4. 保留逻辑上的 REFRESH INCREMENTAL
关键代码（getMTMVDdl）：
```java
public static String getMTMVDdl(MTMV mtmv) throws AnalysisException {
    boolean isIvm = mtmv.isIvm();
    addColNameAndComment(mtmv, sb, isIvm);
    if (!isIvm) {
        addMTMVKeyInfo(mtmv, sb);
    }
}

private static void addColNameAndComment(
        TableIf tableIf, StringBuilder sb, boolean filterIvmRowId) {
    for (Column column : columns) {
        if (filterIvmRowId && Column.IVM_ROW_ID_COL.equals(column.getName())) {
            continue;
        }
        // ... normal column output ...
    }
}
```
### 测试用例
测试文件：ShowCreateMTMVTest.java（新建）
```java
// TC-4-1: INCREMENTAL MV 的 SHOW CREATE 不应包含 UNIQUE KEY(...)
@Test
public void testShowCreateIncrementalMVNoUniqueKey() throws Exception { }
// TC-4-2: INCREMENTAL MV 的 SHOW CREATE 不应暴露 hidden row-id 列
@Test
public void testShowCreateIncrementalMVNoRowIdColumn() throws Exception { }
// TC-4-3: INCREMENTAL MV 的 SHOW CREATE 应包含 REFRESH INCREMENTAL
@Test
public void testShowCreateIncrementalMVContainsRefreshIncremental() throws Exception { }
// TC-4-4: 非 INCREMENTAL MV 的 SHOW CREATE 行为不变
@Test
public void testShowCreateCompleteMVUnchanged() throws Exception { }
// TC-4-5: SHOW CREATE 的输出可以被重新执行（可重放性）
@Test
public void testShowCreateIncrementalMVIsReplayable() throws Exception { }
// TC-4-6: show_hidden_columns=true 时也不暴露 row-id 对 INCREMENTAL MV
@Test
public void testShowCreateIncrementalMVNoRowIdEvenWithShowHidden() throws Exception { }
// TC-4-7: DUP_KEYS MV（非 IVM）走 isDuplicateWithoutKey() 分支不输出 KEY 行
@Test
public void testShowCreateDupKeysMVNoKeyOutput() throws Exception { }
```
### 结论
原解法总体正确，但需要补一句：不仅要跳过 key 子句，还要强制过滤 hidden row-id 列。

---
## 5.7 P0-6：修复 validateColumns() 的 KeysType
### 问题描述
`MTMVPlanUtil.validateColumns()` 对所有 MV 都硬编码 KeysType.DUP_KEYS，但 INCREMENTAL MV 的物理模型是 UNIQUE_KEYS + MOW。
### 当前代码事实
- `MTMVPlanUtil.validateColumns()` 仍硬编码 KeysType.DUP_KEYS。
- finalEnableMergeOnWrite 已经正确传入但未被使用于 KeysType 判断。
### 修正方案
### 修改位置：
- fe/fe-core/src/main/java/org/apache/doris/mtmv/MTMVPlanUtil.java
按真实物理模型传入 KeysType：
```java
private static void validateColumns(
        List<ColumnDefinition> columns, Set<String> keysSet,
        boolean finalEnableMergeOnWrite) throws UserException {
    KeysType keysType = finalEnableMergeOnWrite
            ? KeysType.UNIQUE_KEYS : KeysType.DUP_KEYS;
    Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
    for (ColumnDefinition col : columns) {
        if (!colSets.add(col.getName())) {
            ErrorReport.reportAnalysisException(
                ErrorCode.ERR_DUP_FIELDNAME, col.getName());
        }
        if (col.getType().isVarBinaryType()) {
            throw new AnalysisException(
                "MTMV do not support varbinary type : " + col.getName());
        }
        col.validate(true, keysSet, Sets.newHashSet(),
                finalEnableMergeOnWrite, keysType);
    }
}
```
### 测试用例
测试文件：MTMVPlanUtilTest.java（追加）
```java
// TC-6-1: INCREMENTAL MV 的列校验应使用 UNIQUE_KEYS
@Test
public void testValidateColumnsUsesUniqueKeysForIncremental() throws Exception { }
// TC-6-2: 非 INCREMENTAL MV 的列校验仍使用 DUP_KEYS（行为不变）
@Test
public void testValidateColumnsStillUsesDupKeysForNonIncremental() throws Exception { }
// TC-6-3: INCREMENTAL MV 的 row-id 隐藏列不参与用户列校验
@Test
public void testValidateColumnsExcludesIvmRowIdForIncremental() throws Exception { }
```
### 结论
原解法正确，可以直接实施。风险最低。

---
## 5.9 P0-8：支持显式 REFRESH MATERIALIZED VIEW ... INCREMENTAL / PARTITIONS
### 问题描述
语法上，REFRESH MATERIALIZED VIEW 当前只接受 COMPLETE、AUTO、partitionSpec，并不接受显式 INCREMENTAL，也没有独立的 PARTITIONS refresh mode。同时 `LogicalPlanBuilder.visitRefreshMTMV()` 仍把手工刷新语义编码成 partitions + isComplete，无法区分用户显式写了 AUTO、INCREMENTAL、PARTITIONS 还是什么都没写。
### 当前代码事实
- RefreshMTMVInfo 仍用 isComplete 布尔值表达手工刷新语义。
- AST 无法区分显式 AUTO / INCREMENTAL / PARTITIONS / 默认值。
### 修正方案
### 修改位置：
- fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisParser.g4
- fe/fe-core/src/main/java/org/apache/doris/nereids/parser/LogicalPlanBuilder.java
- fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/RefreshMTMVInfo.java
步骤 1：语法和 AST
```antlr
| REFRESH MATERIALIZED VIEW mvName=multipartIdentifier
    (partitionSpec | COMPLETE | AUTO | INCREMENTAL | PARTITIONS)?
    #refreshMTMV
```
步骤 2：RefreshMTMVInfo 改为显式 RefreshMode
```java
public class RefreshMTMVInfo {
    private final TableNameInfo mvName;
    private List<String> partitions;
    private RefreshMode refreshMode;  // 替换 isComplete

    public enum RefreshMode {
        AUTO, COMPLETE, INCREMENTAL, PARTITIONS, DEFAULT
    }
}
```
步骤 3：analyze 约束矩阵
```java
public void analyze(ConnectContext ctx) {
    MTMV mtmv = ...; // 查出 MV
    boolean isIvm = mtmv.isIvm();
    if (!isIvm
            && (refreshMode == RefreshMode.INCREMENTAL
                || refreshMode == RefreshMode.PARTITIONS)) {
        throw new AnalysisException(
            "Cannot use INCREMENTAL/PARTITIONS refresh on a non-INCREMENTAL MV.");
    }
    if (isIvm && !partitions.isEmpty()) {
        throw new AnalysisException(
            "partitionSpec is not allowed on an INCREMENTAL MV, use PARTITIONS instead.");
    }
}
```
### 测试用例
测试文件：RefreshMTMVCommandTest.java（新建）
```java
// TC-8-1: REFRESH MV ... INCREMENTAL 能被 parser 解析
@Test
public void testParseRefreshIncremental() { }
// TC-8-2: REFRESH MV ... AUTO 仍可被正常解析
@Test
public void testParseRefreshAuto() { }
// TC-8-3: REFRESH MV ... COMPLETE 仍可被正常解析
@Test
public void testParseRefreshComplete() { }
// TC-8-4: REFRESH MV ... PARTITIONS 能被 parser 解析为新 refresh mode
@Test
public void testParseRefreshPartitionsMode() { }
// TC-8-5: 非 INCREMENTAL MV 执行 REFRESH ... INCREMENTAL 应报错
@Test
public void testRefreshIncrementalOnNonIncrementalMVRejected() throws Exception { }
// TC-8-6: 非 INCREMENTAL MV 执行 REFRESH ... PARTITIONS 应被拒绝
@Test
public void testRefreshPartitionsOnNonIncrementalMVRejected() throws Exception { }
// TC-8-7: INCREMENTAL MV 执行旧 partitionSpec 应被拒绝
@Test
public void testRefreshPartitionSpecOnIncrementalMVRejected() throws Exception { }
// TC-8-8: INCREMENTAL MV 执行 REFRESH ... INCREMENTAL 在 analyze 侧应通过
@Test
public void testRefreshIncrementalOnIncrementalMVAccepted() throws Exception { }
// TC-8-9: INCREMENTAL MV 执行 REFRESH ... AUTO / COMPLETE / PARTITIONS 应成功
@Test
public void testRefreshOtherModesOnIncrementalMVAccepted() throws Exception { }
```
### 结论
这条可以做，适合拆成一个独立小 PR。关键不是"加一个关键字"，而是先把 RefreshMTMVInfo 的布尔语义模型替换掉，并按物化视图类型建立完整的约束矩阵。

---
## 5.10 P0-9：明确首刷 bootstrap 语义，与 build mode 解耦
### 问题描述
对于 INCREMENTAL MV，首次刷新始终是全量刷新。BUILD IMMEDIATE / DEFERRED 只影响首刷触发时机，不作为硬限制。

### 当前代码现状
- `MTMVJobManager.postCreateMTMV()`：BUILD IMMEDIATE 时以 `RefreshMode.COMPLETE` 触发首刷，**不会进入 IVM 路径**。已正确。
- `MTMVTask.run()` (line 248)：BUILD DEFERRED 后手动 REFRESH AUTO 时，代码会尝试 IVM → MV 为空无 delta 可应用 → IVM 失败 → fallback 到全量刷新。结果正确但过程不优雅（不必要的 IVM 尝试 + 误导性 fallback 日志）。
- `MTMVRefreshState`：CREATE 后初始值为 `INIT`，首刷成功后变为 `SUCCESS`。

### 修正方案
### 修改位置：
- fe/fe-core/src/main/java/org/apache/doris/job/extensions/mtmv/MTMVTask.java

在 IVM 入口处增加 bootstrap 检测：如果 `refreshState == INIT`（从未成功刷新过），直接跳过 IVM，走全量刷新。

```java
// MTMVTask.java run() 方法
if (mtmv.isIvm()) {
    // Bootstrap: 首次刷新必须全量，不尝试 IVM
    if (mtmv.getStatus().getRefreshState() == MTMVRefreshState.INIT) {
        LOG.info("IVM bootstrap: first refresh for mv={}, skipping IVM, will do full refresh",
            mtmv.getName());
    } else {
        IvmRefreshManager ivmRefreshManager = new IvmRefreshManager();
        IvmRefreshResult ivmResult = ivmRefreshManager.doRefresh(mtmv);
        if (ivmResult.isSuccess()) { return; }
        // RefreshMode 区分 fallback 逻辑（见 P0-11）
    }
}
// 继续走分区/全量刷新路径
```

好处：
- 避免不必要的 IVM 尝试 + 误导性 fallback 日志
- "首刷全量"是显式设计决策，不是 fallback 的副作用
- 首刷失败重试时也不会尝试 IVM

不需要做的事：
- ❌ 不需要限制 BUILD IMMEDIATE（`postCreateMTMV()` 已正确发 RefreshMode.COMPLETE）
- ❌ 不需要限制 BUILD DEFERRED（应保留）
- ❌ 不需要修改 CreateMTMVInfo 或 IvmRefreshManager

### 测试用例
```java
// TC-9-1: BUILD IMMEDIATE + INCREMENTAL，首次刷新走全量，不尝试 IVM
@Test
public void testFirstRefreshIsFullWithBuildImmediate() { }
// TC-9-2: BUILD DEFERRED + INCREMENTAL，首次手工刷新跳过 IVM，走全量
@Test
public void testFirstRefreshSkipsIvmWithBuildDeferred() { }
// TC-9-3: 首刷全量成功后（refreshState=SUCCESS），第二次刷新走 IVM 增量路径
@Test
public void testSecondRefreshIsIncrementalAfterSuccessfulBootstrap() { }
// TC-9-4: 首刷失败后（refreshState=INIT 不变），重试仍走全量，不走 IVM
@Test
public void testRetryAfterFailedBootstrapStillFull() { }
```
### 结论
核心改动：在 IVM 入口加 `refreshState == INIT` 跳过检测。BUILD IMMEDIATE / DEFERRED 均无需限制。编码量小（一个 if + 4 个测试）。

---
## 5.11 P0-10：INCREMENTAL MV 的属性一致性与 binlog 提示
### 问题描述
不再建议把属性写成"统一禁止"或"部分支持矩阵"。所有 MTMV 属性对 INCREMENTAL MV 与之前的逻辑保持一致。如果用户使用 AUTO 方式且希望走增量刷新，必须打开 binlog 开关；未开启时 task info 中应提示。
### 修正方案
### 修改位置：
- fe/fe-core/src/main/java/org/apache/doris/job/extensions/mtmv/MTMVTask.java
- 文档层面
属性行为保持不变：
1. 调度/新鲜度判断层（行为不变）：excluded_trigger_tables，与之前逻辑一致
2. 执行上下文层（行为不变）：workload_group，与之前逻辑一致
3. 分区相关属性（对 INCREMENTAL MV 有效）：refresh_partition_num 与之前逻辑一致；partition_sync_limit、partition_sync_time_unit、partition_date_format 如果指定了 INCREMENTAL，这几个开关有效，需要保留物化视图最近 partition_sync_limit 个分区的数据
4. 创建分析 / rewrite 层（行为不变）：enable_nondeterministic_function、use_for_rewrite、grace_period、async_mv.query_rewrite.consistency_relaxed_tables，与之前逻辑一致
Task info 提示： 如果 SQL 可以增量刷新，但没有开启 binlog，task info 中应提示："SQL 可增量刷新，但未开 binlog，只能分区或全量刷新"。
### 测试用例
```java
// TC-10-1: INCREMENTAL MV 的 excluded_trigger_tables 正常生效
@Test
public void testExcludedTriggerTablesWorksForIncremental() { }
// TC-10-2: INCREMENTAL MV 的 workload_group 正常生效
@Test
public void testWorkloadGroupWorksForIncremental() { }
// TC-10-3: INCREMENTAL MV 的 partition_sync_limit 正常生效
@Test
public void testPartitionSyncLimitWorksForIncremental() { }
// TC-10-4: AUTO 模式下未开 binlog 时 task info 包含提示
@Test
public void testTaskInfoHintWhenBinlogNotEnabled() { }
// TC-10-5: partition_sync_limit 设为非整数值应报错
@Test
public void testPartitionSyncLimitRejectsInvalidValue() { }
// TC-10-6: INCREMENTAL MV 开启 binlog 后 task info 无告警提示
@Test
public void testTaskInfoNoHintWhenBinlogEnabled() { }
```
### 结论
所有 MTMV 属性对 INCREMENTAL MV 保持与之前一致的行为。partition_sync_* 系列属性对 INCREMENTAL MV 有效。

---
## 5.12 P0-11：IVM 失败后的处理策略
### 问题描述
不再需要原文建议的"重试时间"方案（incremental_retry_seconds）或"冷却窗口"方案（ivm_fallback_cooldown_sec）。

### 当前代码现状
`MTMVTask.run()` (line 248-261) 中 IVM 失败后**无条件 fallback** 到分区刷新，不检查 `RefreshMode`：
```java
if (mtmv.isIvm()) {
    IvmRefreshResult ivmResult = ivmRefreshManager.doRefresh(mtmv);
    if (ivmResult.isSuccess()) { return; }
    LOG.warn("IVM refresh fell back...");
    // 直接继续走分区刷新，未检查 taskContext.getRefreshMode()
}
```
`IvmRefreshManager` 已有 `IvmFallbackReason` 枚举（10 个值），但 MTMVTask 里不区分原因。

### 修正方案
IVM 失败按三类场景分别处理，核心区分维度是 **失败原因** × **RefreshMode**：

**场景 1：数据/计划层面的可预期失败**

对应 `IvmFallbackReason`：`SNAPSHOT_ALIGNMENT_UNSUPPORTED`、`PLAN_PATTERN_UNSUPPORTED`、`BINLOG_BROKEN`

含义：基表数据变更导致 delta plan 无法生成，或 binlog 断裂。"IVM 暂时干不了，但全量刷新能兜底"。

| RefreshMode | 行为 |
|---|---|
| AUTO | ✅ 允许 fallback 到分区刷新/全量刷新，LOG.warn |
| INCREMENTAL | ❌ 直接抛异常失败，不 fallback |
| COMPLETE | 不走 IVM 路径，直接全量 |
| PARTITIONS | ✅ 允许 fallback 到分区刷新 |

fallback 后恢复：全量刷新成功后，后续触发仍然先尝试 IVM（因为 `isIvm()` 不变）。

**场景 2：执行层面的失败**

对应 `IvmFallbackReason`：`INCREMENTAL_EXECUTION_FAILED`

含义：delta plan 生成成功了，但执行时失败（SQL 执行报错、事务冲突等）。可能是暂时性故障也可能是 bug。

| RefreshMode | 行为 |
|---|---|
| AUTO | ✅ 允许 fallback，但 LOG.error（比场景1严重——plan 能生成说明理论上应该能执行） |
| INCREMENTAL | ❌ 直接抛异常失败 |

与场景1的区别：场景1是"知道做不了所以不做"，场景2是"做了但失败了"。日志级别和告警优先级不同。

**场景 3：代码 bug / 不可恢复错误**

表现为非 `IvmRefreshResult` 的未捕获异常（如 NPE、ClassCastException）。

| RefreshMode | 行为 |
|---|---|
| 所有模式 | ❌ task 失败并报错，不 fallback |

关键逻辑：当前 `IvmRefreshManager.doRefresh()` 把所有异常都包成了 `IvmRefreshResult.fallback()`，导致场景3也会被静默 fallback。需要修改：对 `RuntimeException`（非 AnalysisException）不包装为 fallback，直接抛出让 task 失败。

### 核心代码改动

**MTMVTask.java** — 按 RefreshMode 区分处理：
```java
if (mtmv.isIvm()) {
    IvmRefreshResult ivmResult = ivmRefreshManager.doRefresh(mtmv);
    if (ivmResult.isSuccess()) {
        return;
    }
    RefreshMode mode = taskContext.getRefreshMode();
    if (mode == RefreshMode.INCREMENTAL) {
        // 严格模式：IVM 失败就是失败，不 fallback
        throw new JobException("INCREMENTAL refresh failed: "
            + ivmResult.getFallbackReason() + " - " + ivmResult.getDetailMessage());
    }
    // AUTO / PARTITIONS 模式：允许 fallback
    if (ivmResult.getFallbackReason() == IvmFallbackReason.INCREMENTAL_EXECUTION_FAILED) {
        LOG.error("IVM execution failed for mv={}, fallback to partition refresh. reason={}",
            mtmv.getName(), ivmResult.getDetailMessage());
    } else {
        LOG.warn("IVM plan/precheck failed for mv={}, fallback to partition refresh. reason={}",
            mtmv.getName(), ivmResult.getFallbackReason());
    }
    // 继续走现有分区刷新逻辑...
}
```

**IvmRefreshManager.doRefresh()** — 区分可恢复 vs 不可恢复异常：
```java
public IvmRefreshResult doRefresh(MTMV mtmv) {
    try {
        IvmRefreshResult prechecked = precheck(mtmv);
        if (!prechecked.isSuccess()) { return prechecked; }
        IvmRefreshContext context = buildContext(mtmv);
        return doRefreshInternal(context);
    } catch (AnalysisException e) {
        // 可预期失败 → 返回 fallback result
        return IvmRefreshResult.fallback(IvmFallbackReason.PLAN_PATTERN_UNSUPPORTED, e.getMessage());
    }
    // RuntimeException 等不可恢复异常 → 不捕获，直接抛给 MTMVTask
    // MTMVTask 的顶层 catch 会让 task 失败
}
```

### 不需要的机制
- ❌ 重试时间 / 冷却窗口：不需要。IVM 失败后要么 fallback（AUTO），要么直接失败（INCREMENTAL）
- ❌ IVM 重试循环：不需要在 IVM 层面重试。现有 `executeWithRetry` 只用于分区刷新的 cloud mode 重试
- ❌ "标记 IVM 暂时不可用"机制：不需要。每次刷新都重新尝试 IVM，因为上次失败的原因可能已经消除

### 结论
不需要冷却窗口或重试时间属性。按失败原因 × RefreshMode 两个维度处理：场景1/2在 AUTO 模式允许 fallback，INCREMENTAL 模式直接失败；场景3所有模式都直接失败。

### 测试用例
测试文件：MTMVIvmFailureTest.java（新建）
```java
// 场景 1：数据/计划层面失败
// TC-11-1: 计划失败 + AUTO 模式 → fallback 到分区刷新
@Test
public void testPlanFailureAutoModeFallbackToPartitionRefresh() { }
// TC-11-2: 计划失败 + INCREMENTAL 模式 → 直接报错，不 fallback
@Test
public void testPlanFailureIncrementalModeThrowsException() { }
// TC-11-3: fallback 全量刷新后，后续触发仍先尝试 IVM
@Test
public void testIvmRetriedAfterFullFallback() { }

// 场景 2：执行层面失败
// TC-11-4: 执行失败 + AUTO 模式 → fallback，LOG.error 级别
@Test
public void testExecutionFailureAutoModeFallbackWithError() { }
// TC-11-5: 执行失败 + INCREMENTAL 模式 → 直接失败
@Test
public void testExecutionFailureIncrementalModeFails() { }

// 场景 3：不可恢复错误
// TC-11-6: RuntimeException → 所有模式下 task 失败，不 fallback
@Test
public void testRuntimeExceptionStopsTaskNoFallback() { }
```

---
## 5.13 P1-1：IVM 刷新成功后补充 IVM progress 更新
### 问题描述
IVM 成功后缺少 IVM progress 元数据的更新。当前回调路径（refresh state、partition snapshot、rewrite cache）对 IVM 和普通 MV 行为一致且正确（见 5.2 P0-1），无需分流。唯一缺少的是 IVM 特有的增量进度元数据（如 delta watermark）的更新。
### 修正方案
### 修改位置：
- fe/fe-core/src/main/java/org/apache/doris/catalog/MTMV.java
- fe/fe-core/src/main/java/org/apache/doris/mtmv/MTMVService.java
实现目标：
- INCREMENTAL 成功时，在现有回调路径基础上**额外**更新 IVM progress 元数据
- 以下路径保持不变，IVM 和普通 MV 行为一致：
  - 更新 refresh state ✅（已有）
  - 更新 partition snapshot ✅（已有，供透明改写判定分区新鲜度）
  - 构建 rewrite cache ✅（已有，见 `MTMV.addTaskResult()`）
### 测试用例
测试文件：MTMVTaskCallbackTest.java（新建）
```java
// TC-13-1: IVM 成功后应更新 refresh state（与普通 MV 一致）
@Test
public void testIvmSuccessUpdatesRefreshState() { }
// TC-13-2: IVM 成功后应额外更新 IVM progress
@Test
public void testIvmSuccessUpdatesIvmProgress() { }
// TC-13-3: IVM 成功后应正常更新 partition snapshot（供透明改写使用）
@Test
public void testIvmSuccessUpdatesPartitionSnapshot() { }
// TC-13-4: IVM 成功后应正常构建 rewrite cache（供透明改写使用）
@Test
public void testIvmSuccessBuildsRewriteCache() { }
```
### 结论
IVM 参与透明改写，回调路径无需分流。唯一额外工作是在现有回调中追加 IVM progress 更新。

---
## 5.14 P1-2：明确不支持原地升级与降级兼容策略
### 问题描述
原文里关于"用 @JsonEnumDefaultValue 默认到 COMPLETE"的想法不对。原因：Doris 这里用的是 Gson，不是 Jackson。即使能默认到 COMPLETE，语义也不正确——INCREMENTAL MV 的物理模型是 UNIQUE_KEYS + MOW，带有隐藏列，按 COMPLETE 逻辑刷新会导致 schema 不匹配或数据损坏。
当前问题代码位置：

| 文件 | 方法/字段 | 说明 |
|------|----------|------|
| `MTMV.java` | `gsonPostProcess()` | Gson 反序列化后回调，需在此检测 refreshMethod |
| `MTMV.java` | `getRefreshInfo()` | 返回 MTMVRefreshInfo，消费方依赖 getRefreshMethod() |
| `MTMVTask.java` | `run()` → `calculateNeedRefreshPartitions()` | 根据 refreshMethod 决定全量/增量 |
| `CreateMTMVInfo.java` | `analyze()` | 校验 refreshMethod 合法性 |
| `MTMVStatus.java` | `canBeCandidate()` | 决定 MV 是否参与透明改写 |

所有消费 getRefreshInfo().getRefreshMethod() 的地方（`canBeCandidate()`、`MTMVTask.run()`、`CreateMTMVInfo.analyze()` 等）都没有针对 null 做防护。
### 修正方案
要明确两件事：
1. 不支持把已有普通 async MV 原地升级成 IVM MV
2. 不支持包含 INCREMENTAL MV 的元数据降级到不支持 IVM 的旧版本
### 修改位置：
- fe/fe-core/src/main/java/org/apache/doris/catalog/MTMV.java
修复方案：
MTMV 的父类 OlapTable 已实现 GsonPostProcessable 接口。在 MTMV 中 override gsonPostProcess()，在 Gson 反序列化完成后检测 refreshMethod == null，标记 MV 为不可用。
关键代码（MTMV.java）：
```java
@Override
public void gsonPostProcess() throws IOException {
    super.gsonPostProcess();
    if (refreshInfo != null && refreshInfo.getRefreshMethod() == null) {
        // 旧版本反序列化时遇到未知 RefreshMethod（如 INCREMENTAL），
        // Gson 默认返回 null。标记为不可用，不静默降级为 COMPLETE。
        LOG.warn("MTMV {} has unknown refreshMethod (possibly from a newer version), "
                + "marking as unavailable", getName());
        this.status.changeStatus(MTMVStatus.State.SCHEMA_CHANGE,
                "Unknown refresh method detected during deserialization");
    }
}
```
不能做的事：
- 不能把未知 refreshMethod 静默回退为 COMPLETE
- 不能用 @JsonEnumDefaultValue（这是 Jackson 注解，Gson 不支持）
### 测试用例
测试文件：MTMVGsonCompatibilityTest.java（新建）
```java
// TC-14-1: refreshMethod 为未知值时，反序列化后 MV 应被标记为不可用
@Test
public void testUnknownRefreshMethodMarksUnavailable() throws Exception { }
// TC-14-2: refreshMethod 为已知值（COMPLETE/AUTO/INCREMENTAL）时，反序列化正常
@Test
public void testKnownRefreshMethodDeserializesNormally() throws Exception { }
// TC-14-3: refreshMethod 为 null 时，canBeCandidate() 不应报 NPE
@Test
public void testNullRefreshMethodDoesNotCauseNPE() throws Exception { }
// TC-14-4: refreshMethod 为 null 时，SHOW CREATE 不应输出 "null"
@Test
public void testNullRefreshMethodShowCreateSafe() throws Exception { }
```
### 结论
降级兼容不应把未知 refresh method 静默回退为 COMPLETE。在 gsonPostProcess() 中检测并标记不可用。

---
## 5.15 分区级进度与透明改写（合并至 PR-9）
### 问题描述
partitioned incremental MV 的受影响分区识别与分区级 progress 表达是可以做到的。INCREMENTAL MV 参与透明改写（见 5.2 P0-1），分区级新鲜度由现有 `MTMVPartitionUtil.isMTMVPartitionSync()` 机制保障。
### 修正方案
### 修改位置：
- 无额外代码修改（INCREMENTAL MV 参与透明改写的行为已在 5.2 P0-1 中测试覆盖）
### 实现要求：
- `MTMV.canBeCandidate()` 对 INCREMENTAL MV 行为与 COMPLETE/AUTO 一致
- `MTMV.addTaskResult()` 对 INCREMENTAL MV 正常构建 `MTMVCache`
- 分区级脏数据通过 isMTMVPartitionSync + grace_period 机制处理
### 测试用例
测试文件：MTMVCanBeCandidateTest.java（与 5.2 P0-1 共用）
```java
// TC-15-1: INCREMENTAL MV 参与透明改写（已由 TC-1-1~TC-1-4 覆盖）
// TC-15-2: 分区级 INCREMENTAL MV 参与透明改写（已由回归测试 TC-1-9 覆盖）
// TC-15-3: 受影响分区识别正确性——仅脏分区被标记
@Test
public void testAffectedPartitionIdentificationCorrectness() { }
// TC-15-4: 分区增削后进度元数据正确更新
@Test
public void testProgressUpdatedAfterPartitionAddDrop() { }
```
### 结论
INCREMENTAL MV 参与透明改写，分区级进度和受影响分区识别通过现有机制保障。

---
## 5.16 P1-3：IVM 可观测性 — mv_infos 与 tasks 新增增量刷新字段
### 问题描述
当前 `mv_infos()` 和 `tasks()` 表函数缺少增量刷新相关的可观测字段。用户无法通过标准查询接口看到增量刷新的进度、状态、执行SQL、错误原因等信息。需要在不影响非 IVM MV 的前提下，补充增量刷新的可观测性。

### 当前代码现状

**mv_infos() 数据链路**：
```
SELECT * FROM mv_infos('database'='db')
  → BE RPC → MetadataGenerator.mtmvMetadataResult()
  → 遍历 MTMV 对象，从 FE 内存字段直接读取
  → MTMV 的元数据通过 Gson 持久化到 EditLog/Image
```
当前 11 列：`Id, Name, JobName, State, SchemaChangeDetail, RefreshState, RefreshInfo, QuerySql, MvProperties, MvPartitionInfo, SyncWithBaseTables`

**tasks() 数据链路**：
```
SELECT * FROM tasks("type"="mv")
  → BE RPC → MetadataGenerator.taskMetadataResult()
  → MTMVJob.queryAllTask() → MTMVTask.getTvfInfo()
  → 从 MTMVTask 对象字段读取（@SerializedName 持久化到 EditLog）
```
当前 19 列：`TaskId, JobId, JobName, MvId, MvName, MvDatabaseId, MvDatabaseName, Status, ErrorMsg, CreateTime, StartTime, FinishTime, DurationMs, TaskContext, RefreshMode, NeedRefreshPartitions, CompletedPartitions, Progress, LastQueryId`

**IVM 路径的可观测性缺陷**：
- MTMVTask 在 IVM 成功时直接 `return`（line 254），`refreshMode`、`lastQueryId`、`completedPartitions` 均为 null
- `IvmRefreshResult` 只携带 `success/fallbackReason/detailMessage`，不携带进度或执行 SQL
- `IvmInfo.baseTableStreams[*].properties` 存储流位置（cursor），但不暴露给用户
- `after()` → `addTaskResult()` 在 IVM 成功时被调用，但 `relation=null`、`partitionSnapshots=null`（P1-1 需修复）

### 修正方案

#### A. mv_infos() 新增字段

在 `MvInfosTableValuedFunction.SCHEMA` 末尾追加 3 个字段：

| 新字段 | 类型 | 语义 | 数据来源 | 非 IVM MV 时的值 |
|--------|------|------|----------|----------------|
| `IncrRefreshEndCursor` | STRING | 当前增量刷新已消费到的位置（按基表×分区） | MTMV 上持久化的 `lastIncrRefreshEndCursor`（见 E） | 空字符串 |
| `IncrRefreshMaxCursor` | STRING | 最近一次任务开始时各基表流的最大可用位置 | MTMV 上持久化的 `lastIncrRefreshMaxCursor`（见 E） | 空字符串 |
| `IncrRefreshQuerySql` | STRING | 最近一次增量刷新实际执行的 delta SQL | MTMV 上持久化的 `lastIncrRefreshSql`（见 E） | 空字符串 |

> ⚠️ **设计决策**：`IncrRefreshMaxCursor` **不做实时查询**（避免流不可用时阻塞 mv_infos），改为从 MTMV 上持久化的最近一次 task 快照读取。

字段值格式（JSON）：
```json
// IncrRefreshEndCursor / IncrRefreshMaxCursor
{
  "lineitem": {"p202601": "20", "p202602": "30"},
  "orders": {"p202601": "10"}
}
```

#### B. tasks() 新增字段

在 `MTMVTask.SCHEMA` 末尾追加 6 个字段：

| 新字段 | 类型 | 语义 | 数据来源 | 非 IVM 任务时的值 |
|--------|------|------|----------|-----------------|
| `IncrRefreshStatus` | STRING | 本次增量刷新结果 | `IvmRefreshResult` 映射 | 空字符串 |
| `IncrRefreshErrorMsg` | STRING | 增量刷新失败原因 | `IvmRefreshResult.getFallbackReason() + detailMessage` | 空字符串 |
| `IncrRefreshStartCursor` | STRING | 本次增量刷新起始位置 | 进入 IVM 块时立即从 `IvmInfo.baseTableStreams` 快照 | 空字符串 |
| `IncrRefreshEndCursor` | STRING | 本次增量刷新结束位置 | IVM 成功后从 `IvmInfo.baseTableStreams` 读取 | 空字符串 |
| `IncrRefreshMaxCursor` | STRING | 本次任务开始时各流的最大可用位置 | 进入 IVM 块时查询 | 空字符串 |
| `IncrRefreshExecuteSql` | STRING | 增量刷新实际执行的 SQL | `IvmDeltaCommandBundle.getCommand()` 序列化 | 空字符串 |

> ⚠️ **非 IVM 任务**所有新字段返回空字符串（与 `FeConstants.null_string` 惯例一致），不引入新的状态值。

`IncrRefreshStatus` 取值：
- `SUCCESS`：增量刷新成功
- `FALLBACK`：尝试增量但回退到分区/全量刷新（ErrorMsg 中包含原因）
- `SKIPPED`：首刷 bootstrap 跳过增量（`refreshState == INIT`）
- 空字符串：非 IVM MV 或手动指定 COMPLETE 模式，未尝试增量

> ⚠️ **ExecuteSql 长度限制**：最大 4096 字符，超出截断并加 `...` 后缀。

#### C. MTMVTask 持久化改动

在 `MTMVTask` 中新增字段：
```java
@SerializedName("irs")
private String incrRefreshStatus;

@SerializedName("ire")
private String incrRefreshErrorMsg;

@SerializedName("irsc")
private String incrRefreshStartCursor;  // JSON

@SerializedName("irec")
private String incrRefreshEndCursor;  // JSON

@SerializedName("irmc")
private String incrRefreshMaxCursor;  // JSON

@SerializedName("irsql")
private String incrRefreshExecuteSql;
```

在 IVM 代码路径中设置这些字段：
```java
// MTMVTask.run() 的 IVM 路径中
if (mtmv.isIvm()) {
    // 进入 IVM 块时立即快照 cursor，无论后续成功与否都记录
    this.incrRefreshStartCursor = snapshotCurrentCursors(mtmv);
    this.incrRefreshMaxCursor = queryMaxCursors(mtmv);

    IvmRefreshResult ivmResult = ivmRefreshManager.doRefresh(mtmv);
    if (ivmResult.isSuccess()) {
        this.incrRefreshStatus = "SUCCESS";
        this.incrRefreshEndCursor = snapshotCurrentCursors(mtmv);
        this.incrRefreshExecuteSql = truncate(
                ivmResult.getExecuteSql(), 4096);
        return;
    }
    this.incrRefreshStatus = "FALLBACK";
    this.incrRefreshErrorMsg = ivmResult.getFallbackReason()
            + ": " + ivmResult.getDetailMessage();
    // 继续分区/全量刷新
}
```

`getTvfInfo()` 中追加 6 列输出（与 SCHEMA 新增列顺序对齐）：
```java
trow.addToColumnValue(new TCell().setStringVal(
        incrRefreshStatus == null ? FeConstants.null_string : incrRefreshStatus));
// ... 依次追加其余 5 个字段
```

#### D. IvmRefreshResult 扩展

为支持可观测性，`IvmRefreshResult` 增加可选字段：
```java
private String executeSql;      // 实际执行的 delta SQL（仅成功时有值）
```

在 `IvmRefreshManager.doRefreshInternal()` 中，execute 成功后从 bundles 提取 SQL：
```java
deltaExecutor.execute(context, bundles);
String executeSql = bundles.stream()
        .map(b -> b.getCommand().toSql())
        .collect(Collectors.joining("; "));
return IvmRefreshResult.success(executeSql);
```

#### E. MTMV 持久化字段（供 mv_infos 读取）

在 `MTMV` 对象上新增持久化字段，在 `addTaskResult()` 路径中写入：
```java
@SerializedName("lirs")
private String lastIncrRefreshSql;

@SerializedName("lirec")
private String lastIncrRefreshEndCursor;

@SerializedName("lirmc")
private String lastIncrRefreshMaxCursor;
```

`addTaskResult()` 中追加：
```java
if (task.getIncrRefreshStatus() != null
        && "SUCCESS".equals(task.getIncrRefreshStatus())) {
    this.lastIncrRefreshSql = task.getIncrRefreshExecuteSql();
    this.lastIncrRefreshEndCursor = task.getIncrRefreshEndCursor();
    this.lastIncrRefreshMaxCursor = task.getIncrRefreshMaxCursor();
}
```

`MetadataGenerator.mtmvMetadataResult()` 追加 3 列：
```java
trow.addToColumnValue(new TCell().setStringVal(
        mv.getLastIncrRefreshEndCursor() == null ? "" : mv.getLastIncrRefreshEndCursor()));
trow.addToColumnValue(new TCell().setStringVal(
        mv.getLastIncrRefreshMaxCursor() == null ? "" : mv.getLastIncrRefreshMaxCursor()));
trow.addToColumnValue(new TCell().setStringVal(
        mv.getLastIncrRefreshSql() == null ? "" : mv.getLastIncrRefreshSql()));
```

### 边界问题与设计决策

| # | 边界问题 | 决策 |
|---|---------|------|
| 1 | `IncrRefreshMaxCursor` 实时查询流可能阻塞 mv_infos | 改为从 MTMV 持久化的最近一次 task 快照读取，不实时查询 |
| 2 | `IncrRefreshExecuteSql` 可能非常长（多 bundle MERGE INTO） | 限制 4096 字符，超出截断加 `...` |
| 3 | `startCursor` 快照时机 | 进入 `if (mtmv.isIvm())` 块时立即快照，无论后续成功与否 |
| 4 | 非 IVM 任务的新字段表示方式 | 使用空字符串（与现有 `FeConstants.null_string` 惯例一致） |
| 5 | mv_infos 的 3 个新字段需要 MTMV 持久化 | 在 `addTaskResult()` 中仅 IVM 成功时写入；MTMV 新增 3 个 `@SerializedName` 字段 |
| 6 | IVM 成功时 `after()` 调 `addTaskResult()` 但 `relation=null` | P1-1 需修复；P1-3 的字段写入在同一 `addTaskResult()` 路径中，不受影响 |
| 7 | 老版本 MTMVTask 反序列化缺少新字段 | Gson 对缺少的 `@SerializedName` 字段默认为 null → `getTvfInfo()` 输出空字符串，向后兼容 |

### 修改位置：
- `fe/.../tablefunction/MvInfosTableValuedFunction.java`：SCHEMA 追加 3 列
- `fe/.../tablefunction/MetadataGenerator.java`：`mtmvMetadataResult()` 追加 3 列填充
- `fe/.../job/extensions/mtmv/MTMVTask.java`：SCHEMA 追加 6 列 + 6 个新字段 + `getTvfInfo()` 追加 + IVM 路径设值
- `fe/.../mtmv/ivm/IvmRefreshResult.java`：扩展 executeSql 字段 + `success(String sql)` 工厂方法
- `fe/.../mtmv/ivm/IvmRefreshManager.java`：`doRefreshInternal()` 提取执行 SQL 传入 result
- `fe/.../catalog/MTMV.java`：3 个新持久化字段 + `addTaskResult()` 中写入

### 测试用例
```java
// TC-16-1: 非 IVM MV 的 mv_infos 新增字段均为空字符串
@Test
public void testMvInfosNonIvmFieldsEmpty() { }
// TC-16-2: IVM MV 成功后 mv_infos 的 IncrRefreshEndCursor 非空
@Test
public void testMvInfosIvmEndCursorAfterSuccess() { }
// TC-16-3: 非 IVM 任务的 tasks 新增字段均为空字符串
@Test
public void testTasksNonIvmFieldsEmpty() { }
// TC-16-4: IVM 成功任务的 IncrRefreshStatus 为 SUCCESS，ExecuteSql 非空
@Test
public void testTasksIvmSuccessStatusAndSql() { }
// TC-16-5: IVM 回退任务的 IncrRefreshStatus 为 FALLBACK，ErrorMsg 包含 FallbackReason
@Test
public void testTasksIvmFallbackStatusAndErrorMsg() { }
// TC-16-6: 首刷跳过增量的任务 IncrRefreshStatus 为 SKIPPED
@Test
public void testTasksBootstrapSkipStatus() { }
// TC-16-7: IncrRefreshExecuteSql 超过 4096 字符时被截断
@Test
public void testTasksExecuteSqlTruncation() { }
// TC-16-8: 老版本 MTMVTask 反序列化后新字段为 null，getTvfInfo 输出空字符串
@Test
public void testOldTaskDeserializationCompat() { }
```

### 结论
通过在 mv_infos 追加 3 个字段、tasks 追加 6 个字段，实现 IVM 增量刷新的完整可观测性。所有新字段对非 IVM MV 返回空字符串，向后兼容。MaxCursor 不做实时查询避免阻塞，ExecuteSql 限长避免爆内存。依赖 P1-1 修复 addTaskResult 路径后才能完整运行。

---
# 6. 不再需要的方案
- incremental_retry_seconds / ivm_fallback_cooldown_sec：不再需要"秒级重试时间"方案或"冷却窗口"方案。按三类失败场景分别处理（详见 5.12）。
- @JsonEnumDefaultValue 默认到 COMPLETE：Doris 用的是 Gson，不是 Jackson。即使能默认到 COMPLETE，语义也不正确（详见 5.14）。
- INCREMENTAL + PARTITION BY 一刀切禁止：不应在 analyze 阶段一刀切禁止，但也不能写成"不能增量时直接退化"（详见 5.3）。
- BUILD IMMEDIATE 作为硬限制：不应作为 INCREMENTAL 的建表硬限制（详见 5.10）。

---
# 7. PR 拆分与上线门禁
## 7.1 工期汇总与排期（04/08 起，排除周末，总计 13 个工作日）

工期说明：
- 编码：AI 辅助编码 + 自测，按实际工作时间估算
- Review：人工 code review + 修改意见迭代，按自然日估算

| PR | 标题 | 对应章节 | 编码 | Review | 状态 |
|----|------|----------|------|--------|------|
| PR-9 | rewrite 治理 + 分区级透明改写 | P0-1 + 5.15 | 3天 | 1~2天 | |
| PR-1 | REFRESH ... INCREMENTAL/PARTITIONS 语法 | P0-8 | 1天 | 1~2天 | 已完成编码 |
| PR-5 | validateColumns KeysType 修复 | P0-6 | 0.5天 | 1天 | |
| PR-4 | SHOW CREATE 隐藏 row-id/UNIQUE KEY | P0-4 | 0.5天 | 1天 | |
| PR-2 | CREATE INCREMENTAL MV 组合校验 | P0-2 + P0-3 | 1天 | 1~2天 | |
| PR-3 | 首刷 bootstrap 语义 | P0-9 | 0.5天 | 1天 | |
| PR-6 | 属性一致性与 binlog 提示 | P0-10 | 0.5天 | 1天 | |
| PR-8 | IVM 失败按 RefreshMode 区分处理 | P0-11 | 0.5天 | 1天 | |
| PR-7 | IVM progress 更新与降级兼容 | P1-1 + P1-2 | 1.5天 | 2天 | |
| PR-10 | IVM 可观测性字段 | P1-3 | 1天 | 1~2天 | |

说明：
- 编码串行（一人开发），Review 可与下一 PR 编码并行
- 04/11（六）、04/12（日）、04/18（六）、04/19（日）为周末，不计入工期
- 编码合计 9 天，含 review 并行后总工期约 14 个工作日（04/08 ~ 04/25）
## 7.2 测试汇总

| PR | 测试文件 | 类型 | 用例数 |
|----|----------|------|--------|
| PR-9 | `MTMVCanBeCandidateTest.java` | FE UT | 6 |
| PR-9 | `test_ivm_transparent_rewrite.groovy` | 回归 | 13 |
| PR-1 | `RefreshMTMVCommandTest.java` | FE UT | 7 |
| PR-1 | `RefreshMTMVInfoAnalyzeTest.java` | FE UT | 7 |
| PR-2 | `CreateMTMVCommandTest.java`（追加） | FE UT | 7 |
| PR-2 | `AlterMTMVTest.java`（追加） | FE UT | 5 |
| PR-3 | 待定 | FE UT + 回归 | ~3 |
| PR-4 | `ShowCreateMTMVTest.java` | FE UT | 5 |
| PR-5 | `MTMVPlanUtilTest.java`（追加） | FE UT | 3 |
| PR-6 | 待定 | FE UT + 回归 | ~4 |
| PR-7 | `MTMVTaskCallbackTest.java` | FE UT | 4 |
| PR-7 | `MTMVGsonCompatibilityTest.java` | FE UT | 4 |
| PR-8 | `MTMVIvmFailureTest.java` | FE UT | 5 |
| PR-10 | `IvmObservabilityTest.java` | FE UT | 8 |
| **合计** | | | **~79** |
## 7.3 上线门禁
在对外公开 INCREMENTAL 之前，至少满足：
- P0 全部完成
- P1-1、P1-2、P1-3 完成
- SHOW CREATE 可重放
- IVM 与普通刷新共享一致的 snapshot 语义（由其他文档覆盖）
- REFRESH MATERIALIZED VIEW ... INCREMENTAL / PARTITIONS 语义稳定
- PARTITION BY 和属性矩阵的文档结论与实际代码一致
- 测试清单中的关键 case 全覆盖
如果只完成 P0，最多把它定义成"有限场景能力"，不建议对外宣传为稳定能力。
如果要对外承诺"真正增量刷新"，必须完成 P2。

### 关键代码位置索引

| 模块 | 文件路径 | 关键类/方法 |
|------|----------|------------|
| MV 定义 | `fe/.../catalog/MTMV.java` | `MTMV`, `gsonPostProcess()`, `canBeCandidate()` |
| 创建信息 | `fe/.../mtmv/CreateMTMVInfo.java` | `analyze()`, `analyzeRefreshMethod()` |
| 刷新信息 | `fe/.../commands/info/RefreshMTMVInfo.java` | `RefreshMode`, `validateRefreshModeCompat()` |
| ALTER 信息 | `fe/.../mtmv/AlterMTMVRefreshInfo.java` | `analyze()` — 禁止切换 INCREMENTAL |
| 任务上下文 | `fe/.../mtmv/MTMVTaskContext.java` | `RefreshMode`, `isComplete()` 兼容 |
| 任务执行 | `fe/.../mtmv/MTMVTask.java` | `run()`, `calculateNeedRefreshPartitions()` |
| 任务管理 | `fe/.../mtmv/MTMVJobManager.java` | `refreshMTMV()`, `onCommitTransaction()` |
| DDL 输出 | `fe/.../catalog/Env.java` | `getMTMVDdl()` — SHOW CREATE |
| 列校验 | `fe/.../mtmv/MTMVPlanUtil.java` | `validateColumns()` — KeysType 修复 |
| IVM 执行 | `fe/.../mtmv/IvmDeltaExecutor.java` | 增量刷新主执行器 |
| IVM 改写 | `fe/.../mtmv/IvmDeltaRewriter.java` | delta plan 改写 |
| IVM 管理 | `fe/.../mtmv/IvmRefreshManager.java` | IVM 刷新协调器 |
| 语法定义 | `fe/.../nereids/DorisParser.g4` | `refreshMTMV` 规则 |
| 语法构建 | `fe/.../nereids/parser/LogicalPlanBuilder.java` | `visitRefreshMTMV()` |
| 透明改写 | `fe/.../mtmv/MTMVStatus.java` | `canBeCandidate()` — 改写候选 |
| mv_infos | `fe/.../tablefunction/MvInfosTableValuedFunction.java` | SCHEMA 定义、mv 元数据列 |
| tasks | `fe/.../tablefunction/TasksTableValuedFunction.java` | 任务表函数入口 |
| 元数据生成 | `fe/.../tablefunction/MetadataGenerator.java` | `mtmvMetadataResult()` — mv_infos 数据填充 |
| IVM 结果 | `fe/.../mtmv/ivm/IvmRefreshResult.java` | 增量刷新结果（含 fallbackReason） |
| IVM 元信息 | `fe/.../mtmv/ivm/IvmInfo.java` | `enableIvm`, `binlogBroken`, `baseTableStreams` |
| IVM 流引用 | `fe/.../mtmv/ivm/IvmStreamRef.java` | `streamType`, `consumerId`, `properties`（cursor） |
