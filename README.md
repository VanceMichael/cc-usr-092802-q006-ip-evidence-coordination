# 协同事务平台

协同事务平台为需要多机构参与的业务提供统一的机构、授权、案件、证据、审批、资源预约、资金分录、消息归并、通知、定时任务和历史回放能力。项目只使用 Python 标准库与 SQLite，适合在单个 Linux 应用容器内运行。

## 目录

- `src/civicflow/`：领域服务、SQLite 持久化、权限和命令行入口。
- `tests/`：核心流程、边界条件和异常路径测试。
- `examples/`：本地演示输入。

## 配置

通过 `CIVICFLOW_DB` 指定 SQLite 文件路径；不设置时命令行使用当前目录下的 `civicflow.sqlite3`。所有时间使用带时区的 ISO 8601 字符串。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 提交水位与历史回放

每次写入（创建、修订、状态转换、保管交接、状态更正等）都会在数据库内取得一个
全局单调递增的整数提交水位 `commit_seq`，随写操作一起返回。水位由 SQLite
序列表在写事务内分配，不依赖进程内计数：即使多笔写入共享同一业务时间
（同一秒内先登记证据、后补录鉴别结论），也具有稳定、可持久化的先后；并发提交
拿到的水位互不相同且连续，服务重启后保持一致。旧库升级时按
（业务时间、实体内版本号、实体类型、实体标识）为既有记录确定性回填水位并重建
审计链，迁移可重复执行；幂等重放不会产生新水位。

截止时点查询 `snapshot(as_of, bound=..., watermark=...)` 支持：

- `last`（默认，向后兼容）：该时点最后可见的版本；
- `first`：该时点最先可见的版本，用于证明某时刻当时实际掌握的材料；
- `at` + `watermark`：精确选择指定水位的版本；
- `snapshot_at_watermark(watermark)`：返回给定全局水位下实体的可见版本，
  移交接收方可凭返回的水位证明某份证据在当时是否已经可见。

案件历史、审计哈希链与列表排序均使用同一水位语义。

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
