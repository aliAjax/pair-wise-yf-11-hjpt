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

## 账目可靠性

- **并发不丢账**：所有写操作经过单条事务队列串行执行「读最新账 → 修改 → 落盘」，
  同时到达的两笔保存逐条保留，不再出现后写覆盖先写。
- **领用互斥**：同一处破损已被未结批次（`status != completed`）领用时，
  新建批次的申请整笔拒绝并返回 `409`（响应体 `conflicts` 列出冲突明细），已成立的批次不受影响；
  批次完工后破损可再次领用。
- **持久化原子写**：写入流程为「写唯一临时文件 → `fsync` → `rename` 替换主账 → `fsync` 目录」，
  返回成功即已落盘，重启后仍可查；断电不会在主账上留下写了一半的 JSON。
- **断电恢复**：启动时清理上次中断遗留的 `.db.json.*.tmp` 临时文件，主账保持完整直接打开
  （未收到成功响应的写入视为未完成，由调用方重新提交即可）。
  若主账本身无法解析，服务拒绝启动并保留现场，不会用空账静默覆盖旧账。

## 验证

```bash
node test/stability.test.js
```

覆盖：并发保存双成功、领用冲突 409 不落账、同破损并发争抢一成一拒、
完工后释放、重启可查、残留临时文件清理、主账损坏保护。

