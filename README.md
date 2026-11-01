# flink-connector-clickhouse-lp

> ClickHouse JDBC dialect for Apache Flink 1.17 — a pluggable `JdbcDialectFactory` extension (JDK 8), behavior aligned with the official ClickHouse Flink connector.

## 这是什么

给 `flink-connector-jdbc` 补上 ClickHouse 方言的**外部扩展 jar**：

```
CREATE TABLE ch_ods_devices (...) WITH (
  'connector' = 'jdbc',
  'url'       = 'jdbc:clickhouse://doris05:38123/traccar',
  'table-name'= 'ods_tc_devices',
  ...
);
```

Flink 1.17 的官方 JDBC 连接器只内置 MySQL / Oracle / PostgreSQL / SQL Server / Derby 五种方言，`jdbc:clickhouse:` 会直接报找不到方言。本项目通过 `JdbcDialectFactory` 的 **SPI** 注册一个 ClickHouse 方言实现，jar 放进 `$FLINK_HOME/lib` 即生效，**不需要 fork 或重新打包连接器**。

- 运行时依赖：`flink-connector-jdbc-3.1.2-1.17.jar` + `clickhouse-jdbc-0.6.5-shaded.jar`（均由父仓库 `software/clickhouse-connector/` 统一分发）
- 本 jar 自身不夹带任何第三方依赖（全部 `provided`），体积几十 KB

## 为什么不用官方 ClickHouse connector

ClickHouse 官方已发布 `com.clickhouse.flink:flink-connector-clickhouse-1.17`，且**模块默认目标就是 Flink 1.17.2**，功能也更完整（原生 client、提交期 schema 校验、完整类型覆盖）。

但它要求 **Java 11+**：Gradle toolchain 固定 11（字节码 major 55），并依赖 `client-v2`（使用 JDK 11 的 `java.net.http`）。本项目所在集群 Flink 运行在 **JDK 8**（major 52），加载即 `UnsupportedClassVersionError`，且改字节码版本也无法绕过传输层依赖。

因此本项目定位明确：

| 阶段 | 方案 |
|---|---|
| 集群 JDK 8（当前） | **本项目**（JDBC + 自研方言） |
| 集群升级 JDK 11+ 后 | 切官方 connector，本项目退役 |

## 与官方 connector 的口径对齐

自研不等于自说自话，**行为口径以官方 README 为准**（详见父仓库计划 §4.2.1）：

- **对齐**：`sink.buffer-flush.max-rows` / `sink.buffer-flush.interval` / `sink.max-retries` / `sink.parallelism` 参数名与语义；反引号标识符引用；**insert-only**（`getUpsertStatement` 返回空）；无 exactly-once，幂等交给 `ReplacingMergeTree` + `FINAL`/`argMax`
- **显式不支持**（不假装支持）：`update/delete`、`sink.batch-failure-strategy`、`sink.max-in-flight-requests`、`sink.max-buffered-requests`、`sink.record.max-bytes`、`sink.timezone`、`clickhouse.client.*` / `clickhouse.server.*` 透传
- **类型支持**：P0 覆盖标量（`BOOLEAN` / 整数 / `DECIMAL` / `FLOAT` / `DOUBLE` / 字符串 / `DATE` / `TIMESTAMP(p)`），P1 再扩 `ARRAY` / `MAP` / `MULTISET` / `ROW`

> ⚠️ 建表时**不要声明 `PRIMARY KEY`**：一旦声明，JDBC 连接器会走 `SELECT + UPDATE/INSERT` 兜底路径，而 ClickHouse 不支持这种 UPDATE，作业会直接失败。

## 构建

容器化构建，**宿主零安装**（不需要 JDK / Maven）：

```bash
bash build.sh 1.0.0        # 产物：build/flink-connector-clickhouse-dialect-1.17-1.0.0.jar
```

- builder 镜像：`maven:3.9-eclipse-temurin-8`（JDK 8，与集群一致）+ 阿里云 maven 镜像
- m2 依赖走命名卷缓存，二次构建可离线复现
- 构建内含断言：jar 必须含 SPI 文件、且不得夹带 `org/apache/flink/connector/jdbc/**`

## 发布

由父仓库 `hadoop-cluster-physical` 的发布闸门统一入库（本仓库不直接写制品）：

```bash
bash ../scripts/flink/release_flink_clickhouse_connector.sh 1.0.0
# → ../software/clickhouse-connector/{3 个 jar, *.sha256, manifest.txt}
```

## 分支策略

| 分支 | 角色 |
|---|---|
| `main` | 稳定主干，**只接受来自 `dev` 的 PR**；每次合并即一个可发布版本（打 tag） |
| `dev` | 日常开发默认分支，长期存在；`feat/*`、`fix/*` 的 PR 目标 |
| `feat/<desc>` | 功能分支，从 `dev` 拉出，完成 → PR 到 `dev` |
| `fix/<desc>` | 缺陷分支，同上 |

发布节奏：`dev` 上验证充分 → PR 到 `main` → 打 tag `vX.Y.Z` → 父仓库 bump 子模块指针。

## 兼容性

| 项 | 版本 |
|---|---|
| Apache Flink | 1.17.2 |
| JDK（编译 + 运行） | 8 |
| flink-connector-jdbc | 3.1.2-1.17 |
| clickhouse-jdbc | 0.6.5（shaded，`ClickHouseClient` 核心类完整） |
| ClickHouse Server | 24.8.x（实测） |

## 状态

S0.5 骨架完成（2026-11-01）：方言 + 工厂 + RowConverter + SPI 注册 + 容器化构建已跑通。

- 单测 10 项全绿（URL 匹配 / 反引号引用 / SQL 生成 / upsert 置空 / 类型白名单 / 非法类型拒绝 / SPI 文件存在 / ServiceLoader 可发现）
- 产物 `flink-connector-clickhouse-dialect-1.17-1.0.0.jar`：7 KB、3 个 class、含 SPI 文件、**不含**连接器类
- 下一步：接入父仓库发布闸门（`software/clickhouse-connector/`）→ Flink 集群 POC（单条消息写入 `traccar.ods_tc_devices`）

计划与验收口径见父仓库 `docs/plan/20261101-实时链路C0接入计划（ClickHouse-Flink-Dinky三侧准备）.md`。

