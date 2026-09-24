# 古籍拓片缺损修补API

纯后端零依赖Node服务，使用 `data/db.json` 持久化拓片、缺损项和修补批次。

## 启动

```bash
PORT=3020 node server.js
```

## 主要接口

- `GET /health`
- `GET /rubbings`
- `POST /rubbings`
- `GET /rubbings/:id/damages`
- `POST /rubbings/:id/damages`
- `GET /damages?status=&type=`
- `PATCH /damages/:id`
- `GET /batches`
- `POST /batches`
- `GET /batches/:id`
- `POST /batches/:id/complete`

## 闭环示例

```bash
curl http://127.0.0.1:3020/damages?status=pending
curl -X POST http://127.0.0.1:3020/batches \
  -H 'Content-Type: application/json' \
  -d '{"name":"六月小批修补","damageIds":["damage_demo_1","damage_demo_2"]}'
```

## 并发与持久化保证

- 所有写入在进程内全局互斥队列中逐条执行（读-改-写事务），并发请求不会互相覆盖，先到先落账。
- 每笔写入先写 `data/wal/` 下的写前日志（临时文件 + `fsync` + `rename` 为提交点），再用
  临时文件 + `rename` 原子替换 `data/db.json`，最后清理日志（checkpoint）。
- 重启时自动恢复：已提交但未并入快照的日志按序号重放；未提交的 `*.tmp` 视为未完成写入
  直接丢弃，由调用方重新提交。恢复期间会用日志重建一份完整快照。
- 快照损坏且没有任何已提交日志时，服务拒绝启动（停机保护），不会再用演示数据静默覆盖旧账。
- `POST /batches` 对申请的缺损项做未结批次占用检查：任一处破损已被 `open` 批次领用时，
  整笔申请返回 `409`（不落账，响应体 `conflicts` 给出占用方批次），已成立的批次不受影响。
  批次完工（`completed`）后其占用自然释放。
