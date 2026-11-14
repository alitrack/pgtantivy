# pgtantivy P3 预研：用 pgrx 0.13 实现 PostgreSQL Index Access Method 的可行性

日期：2026-11-14 · 结论：**可行，且有同栈（pgrx+tantivy）成熟先例 ParadeDB pg_search**。pgrx 0.13.1 无需升级即可在 PG17 上实现完整 index AM。

## 1. 证据链

### 1.1 pgrx 0.13.1 绑定完整（本机 cargo registry 实测）
`~/.cargo/registry/src/.../pgrx-pg-sys-0.13.1/src/include/pg17.rs:11478` 存在完整 `pub struct IndexAmRoutine`，**44 个 am 字段**，包括 PG17 专属的：
- `aminsertcleanup`（line 11504，PG17 新增的批量插入清理回调）
- `amcanbuildparallel`（line 11495）

即绑定由 bindgen 从官方头文件生成，PG17 新字段自动可用，**不构成升级 pgrx 的理由**（加 PG18 才需要升级）。

### 1.2 同栈先例：ParadeDB pg_search（paradedb/paradedb，main 分支）
pg_search 正是「pgrx + tantivy 的 PG index AM」（`CREATE INDEX ... USING paradedb / bm25`）。其 `pg_search/src/postgres/mod.rs` 给出完整落地模式：

```rust
#[pg_extern(sql = "
CREATE FUNCTION bm25_handler(internal) RETURNS index_am_handler
  PARALLEL SAFE IMMUTABLE STRICT COST 0.0001
  LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
CREATE ACCESS METHOD paradedb TYPE INDEX HANDLER bm25_handler;
")]
fn bm25_handler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut amroutine = unsafe { PgBox::from_pg(pgrx::makeNode!(pg_sys::IndexAmRoutine).get()) };
    amroutine.amstrategies = 2;
    amroutine.amcanmulticol = true;
    amroutine.amoptionalkey = true;
    amroutine.amsearcharray = true;
    amroutine.amvalidate = Some(validate::amvalidate);
    amroutine.ambuild = Some(build::ambuild);
    // ... 各回调
    amroutine
}
```

要点：
- **`#[pg_extern(sql = "...")]` 手写 handler 注册 SQL** 是 pgrx 0.13 的正路（pgrx 没有更高层的 index-AM 宏）；`@FUNCTION_NAME@` 由 pgrx 替换成自动生成的 wrapper 符号名，因此不踩 42723 重复声明坑。
- 回调本体是独立模块里的 `#[pg_guard] pub extern "C-unwind" fn`（scan.rs 模式：`ambeginscan` 用 `pg_sys::RelationGetIndexScan` 建 scandesc；`amrescan` 把 `ScanKeyData`（sk_strategy/sk_attno/sk_argument）翻译成查询对象；`amgettuple`/`amgetbitmap` 从 tantivy fast field 取 ctid 返回 TID）。
- pg_search 用到的 flag 组合直接可抄：`amoptionalkey=true, amcanmulticol=true, amsearcharray=true, amstrategies=2, amsupport=0, amkeytype=InvalidOid`。
- PG17 上他们启用了 `aminsertcleanup + amcanbuildparallel`（cfg 门控），PG16 用 fake stub——可选增强，MVP 可不做。

### 1.3 pgrx 没有安全层 —— 全部是 unsafe/pg_sys 直写
Index AM 是 pgrx 尚无安全抽象的区域（不像 `#[pg_extern]`/FDW 有 helper）：`Relation`, `IndexInfo`, `IndexScanDesc`, `ScanKey`, `TIDBitmap` 都是裸 pg_sys 操作。工作量在 C 级别，只是语言换成 Rust + `#[pg_guard]` panic 防护。这是先例（pg_search 数千行 postgres/ + storage/ 代码）证明过的模式。

## 2. 必需回调清单（PG 文档 + pg_search 对照）

必需：`ambuild`, `ambuildempty`, `aminsert`, `ambeginscan`, `amrescan`, `amgettuple`, `amendscan`, `ambulkdelete`, `amvacuumcleanup`, `amcostestimate`, `amvalidate`。
MVP 建议从简/可选：`amoptions`（要 WITH (...) reloptions 就要写）、`amgetbitmap`（FTS 强烈建议，bitmap scan 是主力）、`amcanreturn`（index-only，可先不写=永远回表）、`ammarkpos/amrestrpos`（amcanbackward=false 则不需要）、并行扫描三件套、`aminsertcleanup`。

## 3. pgtantivy 现有资产 → AM 概念的映射

| 现有（函数面 v0.2.0） | AM 对应 |
|---|---|
| `pgtantivy_create`（建 sidecar 目录+schema） | `ambuild`（建空 tantivy index + 建目录，从 reloptions 读字段配置） |
| `pgtantivy_add/remove` + xact callback 缓冲、PreCommit 单次 commit、Abort 清空 | `aminsert`（缓冲 (ctid, doc)，PreCommit flush —— **机制完全同构，直接复用**） |
| delete_term 按 id | `ambulkdelete`（callback 逐 ItemPointer → `delete_term(ctid)`） |
| `pgtantivy_search` 返回 pk | `ambeginscan/amrescan/amgettuple`（ScanKey→tantivy query；返回 heap TID 而非 pk） |
| trigger 同步 | 不再需要（AM insert 路径自动覆盖所有 DML） |

关键语义变化：AM 的身份键从「用户 PK」变成 **ctid**（heap TID）。需把 ctid 存成 tantivy 字段（`INDEXED | STORED | FAST`——沿用 id 字段的老坑：FAST-only 会让 delete_term 静默失效）。非 index-only scan 下可见性由 executor 回表判断，AM 只管 TID 正确。

## 4. 主要风险/难点（按严重度）

1. **存储与 WAL（最大架构分叉）**：PG 要求 AM 变更走 WAL 以保证崩溃一致/物理复制。pg_search 为此把 tantivy segment 存进 index relation 自身（自写 SMR/block 存储 + MetaPage，工程量最大部分）。pgtantivy 目前是 MmapDirectory sidecar —— MVP 可继续用 sidecar（按 relfilenode 命名），代价是**不 WAL、不复制、崩溃后需 REINDEX**（可接受的原型限制，需文档化；类似 pgroonga 早期）。这是决定 P3 工作量数量级的第一个决策点。
2. **planner 接入面**：要 `CREATE INDEX ... USING` 后被规划器选中，需 amcostestimate + 至少一个操作符（如 `@@@`）+ OPERATOR FAMILY/CLASS 手写 SQL（pg_amop/pg_amproc、strategy 号与 amstrategies 对齐）+ amvalidate 放行该 opclass。这是一整块非 Rust 的 SQL 工程，pg_search 也全做了。MVP 也可先让现有 SQL 函数直接走 AM 内部（函数面保留），planner 面后置。
3. **MVCC 多版本**：同一行 UPDATE 会 insert 新 TID 版本，旧版本靠 vacuum 的 ambulkdelete 清理（不是事务回调）；scan 侧由回表判可见性，AM 自身可暂不做 SegmentView 级 MVCC（pg_search 做了，属于增强）。
4. **amoptions/reloptions**：字段配置、tokenizer 选择要从函数参数挪到 `WITH (...)`，需手写 reloptions 解析（`transformRelOptions` + 自解析，pg_search 有 options 模块可参照）。
5. **ambuildempty / unlogged 表**：小活但必需。
6. **单 writer/内存**：aminsert 每 tuple 调用，沿用现有「事务内缓冲 + PreCommit 一次 commit」即可，pg_search 的 mutable-layer 是更重的优化，不必学。

## 5. 工作量判断

- MVP AM（sidecar 存储、amgetbitmap、@@@ 操作符 + opclass、无并行、无 insertcleanup）：在现有 lib.rs 资产上约 **1.5–3k 行 Rust + 数百行 SQL**，模式全部有 pg_search 逐文件可抄（build.rs/insert.rs/scan.rs/delete.rs/vacuum.rs/options.rs/validate.rs/cost.rs）。
- 生产级（WAL 内存储、MVCC-aware scan、并行、ambulkdelete 增量 merge）：参照 pg_search 是**年量级投入**，他们为此 fork 了 tantivy。

## 6. 建议

1. P3 按 MVP 切：sidecar 存储 + ctid 身份键 + aminsert(缓冲复用现有 xact callback) + scan 三件套 + bulkdelete/vacuumcleanup + 简化 amcostestimate + @@@ opclass。
2. 明确文档化「非 WAL：崩溃/主备切换后 REINDEX」限制，把 SMR-in-relation 列为 P4+ 评估项。
3. 保持 pgrx 0.13.1 不动；若同期要 PG18 再升级（0.19+ 线，迁移清单见 skill）。
4. 参考代码锚点：paradedb/paradedb main 分支 `pg_search/src/postgres/{mod,scan,index_only}.rs`（已存 /tmp/paradedb-am/ 本地副本）。

## 附：已核实事实清单

- pgrx 0.13.1 pg17 绑定含 44 字段 IndexAmRoutine，含 aminsertcleanup/amcanbuildparallel（本机 registry 源码 line 11478/11504/11495）。
- docs.rs pgrx 0.13.1 默认页显示的 42 字段版是默认 PG 版本（无 PG17 字段），以本机 registry 为准。
- pg_search main 用 pgrx =0.20.0 + 自 fork 的 tantivy；但其 handler/scan 模式在 0.13 时代（pg_bm25 0.9/0.11）即已存在，`#[pg_extern(sql=)]` + `@FUNCTION_NAME@` 为 0.13 可用特性。
- 本机 pgtantivy（/mnt/d/wsl2/pgtantivy，v0.2.0，pgrx=0.13.1, tantivy=0.24.2, 单 lib.rs 22.7K）。
