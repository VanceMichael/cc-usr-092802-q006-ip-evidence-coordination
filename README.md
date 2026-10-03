# 协同事务平台

协同事务平台为需要多机构参与的业务提供统一的机构、授权、案件、证据、审批、资源预约、资金分录、消息归并、通知、定时任务和历史回放能力。项目只使用 Python 标准库与 SQLite，适合在单个 Linux 应用容器内运行。

## 目录

- `src/civicflow/`：领域服务、SQLite 持久化、权限和命令行入口。
- `tests/`：核心流程、边界条件和异常路径测试。
- `examples/`：本地演示输入。

## 配置

通过 `CIVICFLOW_DB` 指定 SQLite 文件路径；不设置时命令行使用当前目录下的 `civicflow.sqlite3`。所有时间使用带时区的 ISO 8601 字符串。

## 版本水位与截止时点查询

每次实体写入（创建或修订）都会在写事务内从数据库持久化计数器 `sequence_counters` 分配一个全局递增的写入水位 `write_seq`，并记录在 `entities`、`entity_versions` 与审计条目明细中。水位不依赖进程内计数：并发写入由 `BEGIN IMMEDIATE` 事务串行化，服务重启后从数据库中的计数继续推进，因此即使多次写入共享同一业务秒，也有稳定、可持久化的先后顺序。

- `history(...)` 按 `write_seq` 升序返回全部版本，每条记录都带 `write_seq`。
- `snapshot(..., as_of=...)` 默认 `mode="last"`，返回截止时点最后可见的版本，与既有行为一致。
- `mode="first"` 返回截止时点前最近一个业务时点上最先写入（最先可见）的版本，用于回答“那一秒最开始掌握了什么”。
- `mode="watermark", watermark=N` 精确返回水位为 N 的版本；若该版本在 `as_of` 时点尚不可见或不存在，则返回未找到。移交接收方可凭记录中返回的 `write_seq` 证明某份证据在当时是否已经可见。
- 审计链条目在 `detail_json` 中携带同一水位，字段裁剪（`redact_record`）作用于上述查询结果，全部使用同一排序语义。

数据库升级是幂等的：初始化时为旧表补充 `write_seq` 列，按既有历史行的插入顺序（rowid）生成确定水位并播种计数器，既有历史记录保持不变；迁移可重复执行而不会改变已分配的水位。幂等重放（相同请求键）直接返回首次写入时保存的响应，不会分配新水位。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 编译或构建

```bash
PYTHONPATH=src python3 -m compileall -q src
```

## 使用

初始化数据库并运行离线演示：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 demo
```

查看当前案件：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 list-cases
```
