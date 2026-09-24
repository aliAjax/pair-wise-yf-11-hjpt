const http = require("http");
const { readFile, writeFile, mkdir, readdir, rename, unlink } = require("fs/promises");
const fs = require("fs");
const path = require("path");

const PORT = Number(process.env.PORT || 3020);
const DATA_DIR = path.join(__dirname, "data");
const DB_FILE = path.join(DATA_DIR, "db.json");
const WAL_DIR = path.join(DATA_DIR, "wal");

const initialData = {
  rubbings: [
    {
      id: "rubbing_demo",
      code: "TP-清-014",
      source: "地方碑刻残页",
      paperSize: "42x68cm",
      note: "边缘有旧折痕",
      createdAt: new Date().toISOString()
    }
  ],
  damages: [
    {
      id: "damage_demo_1",
      rubbingId: "rubbing_demo",
      position: "左上角第3列题字旁",
      type: "虫蛀孔",
      beforePhotoUrl: "https://example.local/before-014-1.jpg",
      afterPhotoUrl: "",
      status: "pending",
      repairNote: "",
      batchId: null,
      createdAt: new Date().toISOString(),
      repairedAt: null
    },
    {
      id: "damage_demo_2",
      rubbingId: "rubbing_demo",
      position: "下边缘中央",
      type: "撕裂",
      beforePhotoUrl: "https://example.local/before-014-2.jpg",
      afterPhotoUrl: "",
      status: "pending",
      repairNote: "",
      batchId: null,
      createdAt: new Date().toISOString(),
      repairedAt: null
    }
  ],
  batches: []
};

const routes = [
  "GET /health",
  "GET /rubbings",
  "POST /rubbings",
  "GET /rubbings/:id/damages",
  "POST /rubbings/:id/damages",
  "GET /damages?status=&type=",
  "PATCH /damages/:id",
  "GET /batches",
  "POST /batches",
  "GET /batches/:id",
  "POST /batches/:id/complete"
];

/* ------------------------------------------------------------------ */
/* 持久化层：写前日志(WAL) + 原子快照替换 + 互斥串行写                  */
/*                                                                     */
/* 每笔写事务的落盘顺序：                                               */
/*   1. 写 data/wal/<seq>.json.tmp，fsync 文件                         */
/*   2. rename 为 data/wal/<seq>.json，fsync 目录 —— 此步为“提交点”    */
/*   3. 用临时文件+rename 原子替换 data/db.json                        */
/*   4. 删除已并入快照的 WAL（checkpoint）                             */
/* 断电时：已提交的 WAL 在重启恢复时重放；未 rename 的 .tmp 视为        */
/* 未完成写入，重启时丢弃，由客户端/下一次处理重新提交。               */
/* ------------------------------------------------------------------ */

let db = null;
let seq = 0;
let writeChain = Promise.resolve();

function fsyncDir(dir) {
  return new Promise((resolve, reject) => {
    fs.open(dir, "r", (openErr, fd) => {
      if (openErr) return reject(openErr);
      fs.fsync(fd, (syncErr) => {
        fs.close(fd, () => (syncErr ? reject(syncErr) : resolve()));
      });
    });
  });
}

async function writeFileAtomic(target, contents) {
  const tmp = `${target}.${process.pid}.${Date.now().toString(36)}.${Math.random().toString(36).slice(2, 8)}.tmp`;
  await writeFile(tmp, contents);
  await fsyncFile(tmp);
  await rename(tmp, target);
  await fsyncDir(path.dirname(target));
}

function fsyncFile(file) {
  return new Promise((resolve, reject) => {
    fs.open(file, "r", (openErr, fd) => {
      if (openErr) return reject(openErr);
      fs.fsync(fd, (syncErr) => {
        fs.close(fd, () => (syncErr ? reject(syncErr) : resolve()));
      });
    });
  });
}

async function readSnapshot() {
  try {
    const raw = await readFile(DB_FILE, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    if (err && err.code === "ENOENT") return null;
    // 文件存在却解析失败，说明上次落盘被中途打断或外部损坏。
    // 绝不用初始数据静默覆盖（那会丢账），交给上层尝试用 WAL 恢复。
    const error = new Error(`账目快照 ${DB_FILE} 无法解析，请先备份后排查：${err.message}`);
    error.code = "SNAPSHOT_CORRUPT";
    error.cause = err;
    throw error;
  }
}

async function recover() {
  await mkdir(WAL_DIR, { recursive: true });

  const entries = await readdir(WAL_DIR);
  const committed = [];
  let discardedTemp = 0;

  for (const entry of entries) {
    const full = path.join(WAL_DIR, entry);
    if (entry.endsWith(".tmp")) {
      await unlink(full);
      discardedTemp += 1;
      continue;
    }
    const match = entry.match(/^(\d+)\.json$/);
    if (!match) continue;
    const record = JSON.parse(await readFile(full, "utf8"));
    committed.push({ seq: Number(match[1]), file: full, record });
  }
  committed.sort((a, b) => a.seq - b.seq);

  let snapshot;
  try {
    snapshot = await readSnapshot();
  } catch (err) {
    if (err.code !== "SNAPSHOT_CORRUPT") throw err;
    // 快照损坏但有已提交日志时，可以靠日志恢复到最后一次提交后的状态
    if (committed.length === 0) {
      throw new Error(
        `账目快照 ${DB_FILE} 已损坏且没有可用于恢复的已提交日志，已停机保护，请人工核查 data/ 目录。`
      );
    }
    console.warn(`[recover] 快照损坏，将用 ${committed.length} 条已提交日志重建账目。`);
    snapshot = clone(initialData);
  }
  if (snapshot === null) snapshot = clone(initialData);

  for (const item of committed) {
    snapshot = item.record.after;
  }
  const maxSeq = committed.reduce((max, item) => Math.max(max, item.seq), 0);
  seq = maxSeq;

  // 重放后重写一次快照，保证 db.json 完整可开；随后清理已并入的日志。
  await writeFileAtomic(DB_FILE, JSON.stringify(snapshot, null, 2));
  await Promise.all(committed.map((item) => unlink(item.file)));
  if (committed.length) await fsyncDir(WAL_DIR);
  await cleanupStrayTmp(DATA_DIR);

  db = snapshot;
  if (committed.length) {
    console.log(`[recover] 重放 ${committed.length} 条已提交写入，账目恢复到最新状态。`);
  }
  if (discardedTemp) {
    console.log(`[recover] 丢弃 ${discardedTemp} 个未完成（断电前未提交）的写入，需要时请重新提交。`);
  }
}

async function cleanupStrayTmp(dir) {
  const entries = await readdir(dir);
  await Promise.all(
    entries
      .filter((entry) => entry.startsWith(path.basename(DB_FILE)) && entry.endsWith(".tmp"))
      .map((entry) => unlink(path.join(dir, entry)).catch(() => {}))
  );
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

/**
 * 在全局互斥队列中执行一笔读-改-写事务。
 * mutation 在深拷贝上操作，返回结果对象 { data, reply }。
 * 提交完成后才替换内存账本，任何失败都不会污染已有状态。
 */
async function withTransaction(mutation) {
  return new Promise((resolve, reject) => {
    writeChain = writeChain.then(async () => {
      const working = clone(db);
      const result = await mutation(working);
      if (result && result.skipCommit) return result;

      const nextSeq = ++seq;
      const walName = `${String(nextSeq).padStart(12, "0")}.json`;
      const walFinal = path.join(WAL_DIR, walName);
      const walTmp = `${walFinal}.tmp`;

      // 1) 写日志并 fsync
      await writeFile(walTmp, JSON.stringify({ seq: nextSeq, at: new Date().toISOString(), after: working }));
      await fsyncFile(walTmp);
      // 2) rename 提交日志并 fsync 目录
      await rename(walTmp, walFinal);
      await fsyncDir(WAL_DIR);
      // 3) 原子替换快照
      await writeFileAtomic(DB_FILE, JSON.stringify(working, null, 2));
      // 4) checkpoint：已并入快照的日志可删除
      await unlink(walFinal);
      await fsyncDir(WAL_DIR);

      db = working;
      return result;
    }).then(resolve, reject);
  });
}

function send(res, status, body) {
  res.writeHead(status, { "Content-Type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body, null, 2));
}

async function parseBody(req) {
  let raw = "";
  for await (const chunk of req) raw += chunk;
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    const error = new Error("请求体必须是合法JSON");
    error.status = 400;
    throw error;
  }
}

function makeId(prefix) {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function required(body, fields) {
  const missing = fields.filter((field) => body[field] === undefined || body[field] === "");
  if (missing.length) {
    const error = new Error(`缺少字段：${missing.join(", ")}`);
    error.status = 400;
    throw error;
  }
}

function findRubbing(ledger, rubbingId) {
  const rubbing = ledger.rubbings.find((item) => item.id === rubbingId);
  if (!rubbing) {
    const error = new Error("拓片不存在");
    error.status = 404;
    throw error;
  }
  return rubbing;
}

function enrichBatch(ledger, batch) {
  const damages = ledger.damages.filter((item) => batch.damageIds.includes(item.id));
  return {
    ...batch,
    damages,
    total: damages.length,
    repaired: damages.filter((item) => item.status === "repaired").length,
    pending: damages.filter((item) => item.status !== "repaired").length
  };
}

async function handle(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = url.pathname;

  if (req.method === "GET" && pathname === "/health") {
    return send(res, 200, { ok: true, service: "rubbing-repair-api", routes });
  }

  if (req.method === "GET" && pathname === "/rubbings") {
    const data = db.rubbings.map((rubbing) => {
      const damages = db.damages.filter((item) => item.rubbingId === rubbing.id);
      return {
        ...rubbing,
        damageCount: damages.length,
        pendingDamages: damages.filter((item) => item.status !== "repaired").length
      };
    });
    return send(res, 200, { data });
  }

  if (req.method === "POST" && pathname === "/rubbings") {
    const body = await parseBody(req);
    required(body, ["code", "source", "paperSize"]);
    const result = await withTransaction((ledger) => {
      const rubbing = {
        id: makeId("rubbing"),
        code: body.code,
        source: body.source,
        paperSize: body.paperSize,
        note: body.note || "",
        createdAt: new Date().toISOString()
      };
      ledger.rubbings.push(rubbing);
      return { data: rubbing };
    });
    return send(res, 201, { data: result.data });
  }

  const rubbingDamagesMatch = pathname.match(/^\/rubbings\/([^/]+)\/damages$/);
  if (rubbingDamagesMatch && req.method === "GET") {
    const rubbingId = rubbingDamagesMatch[1];
    findRubbing(db, rubbingId);
    return send(res, 200, { data: db.damages.filter((item) => item.rubbingId === rubbingId) });
  }

  if (rubbingDamagesMatch && req.method === "POST") {
    const rubbingId = rubbingDamagesMatch[1];
    const body = await parseBody(req);
    required(body, ["position", "type", "beforePhotoUrl"]);
    const result = await withTransaction((ledger) => {
      findRubbing(ledger, rubbingId);
      const damage = {
        id: makeId("damage"),
        rubbingId,
        position: body.position,
        type: body.type,
        beforePhotoUrl: body.beforePhotoUrl,
        afterPhotoUrl: "",
        status: "pending",
        repairNote: "",
        batchId: null,
        createdAt: new Date().toISOString(),
        repairedAt: null
      };
      ledger.damages.push(damage);
      return { data: damage };
    });
    return send(res, 201, { data: result.data });
  }

  if (req.method === "GET" && pathname === "/damages") {
    const status = url.searchParams.get("status");
    const type = url.searchParams.get("type");
    const data = db.damages.filter((item) => (!status || item.status === status) && (!type || item.type === type));
    return send(res, 200, { data });
  }

  const damagePatchMatch = pathname.match(/^\/damages\/([^/]+)$/);
  if (damagePatchMatch && req.method === "PATCH") {
    const damageId = damagePatchMatch[1];
    const body = await parseBody(req);
    const result = await withTransaction((ledger) => {
      const damage = ledger.damages.find((item) => item.id === damageId);
      if (!damage) return { skipCommit: true, notFound: true, data: null };
      Object.assign(damage, {
        position: body.position ?? damage.position,
        type: body.type ?? damage.type,
        beforePhotoUrl: body.beforePhotoUrl ?? damage.beforePhotoUrl,
        afterPhotoUrl: body.afterPhotoUrl ?? damage.afterPhotoUrl,
        status: body.status ?? damage.status,
        repairNote: body.repairNote ?? damage.repairNote
      });
      damage.repairedAt = damage.status === "repaired" ? new Date().toISOString() : damage.repairedAt;
      return { data: damage };
    });
    if (result.notFound) return send(res, 404, { error: "缺损项不存在" });
    return send(res, 200, { data: result.data });
  }

  if (req.method === "GET" && pathname === "/batches") {
    return send(res, 200, { data: db.batches.map((batch) => enrichBatch(db, batch)) });
  }

  if (req.method === "POST" && pathname === "/batches") {
    const body = await parseBody(req);
    required(body, ["name", "damageIds"]);
    if (!Array.isArray(body.damageIds) || body.damageIds.length === 0) {
      return send(res, 400, { error: "damageIds必须是非空数组" });
    }
    const uniqueIds = [...new Set(body.damageIds)];
    if (uniqueIds.length !== body.damageIds.length) {
      return send(res, 400, { error: "damageIds中存在重复的缺损项" });
    }

    const result = await withTransaction((ledger) => {
      const invalid = uniqueIds.filter((id) => !ledger.damages.find((damage) => damage.id === id));
      if (invalid.length) {
        return { skipCommit: true, status: 400, error: `缺损项不存在：${invalid.join(", ")}` };
      }
      // 同一处破损被未结批次领用时，后到的申请不落账；已成立的批次不受影响。
      const heldByOpenBatch = new Map();
      for (const batch of ledger.batches) {
        if (batch.status !== "open") continue;
        for (const id of batch.damageIds) heldByOpenBatch.set(id, batch);
      }
      const conflicts = uniqueIds
        .filter((id) => heldByOpenBatch.has(id))
        .map((id) => {
          const damage = ledger.damages.find((item) => item.id === id);
          return { damageId: id, position: damage.position, batchId: heldByOpenBatch.get(id).id, batchName: heldByOpenBatch.get(id).name };
        });
      if (conflicts.length) {
        return {
          skipCommit: true,
          status: 409,
          error: "以下缺损项已被未结批次领用，本批次申请未落账",
          conflicts
        };
      }

      const batch = {
        id: makeId("batch"),
        name: body.name,
        status: "open",
        damageIds: uniqueIds,
        note: body.note || "",
        createdAt: new Date().toISOString(),
        completedAt: null
      };
      ledger.batches.push(batch);
      ledger.damages.forEach((damage) => {
        if (uniqueIds.includes(damage.id)) {
          damage.batchId = batch.id;
          damage.status = "in_repair";
        }
      });
      return { status: 201, data: enrichBatch(ledger, batch) };
    });

    if (result.error) return send(res, result.status, { error: result.error, conflicts: result.conflicts });
    return send(res, result.status, { data: result.data });
  }

  const batchMatch = pathname.match(/^\/batches\/([^/]+)$/);
  if (batchMatch && req.method === "GET") {
    const batch = db.batches.find((item) => item.id === batchMatch[1]);
    if (!batch) return send(res, 404, { error: "修补批次不存在" });
    return send(res, 200, { data: enrichBatch(db, batch) });
  }

  const completeMatch = pathname.match(/^\/batches\/([^/]+)\/complete$/);
  if (completeMatch && req.method === "POST") {
    const batchId = completeMatch[1];
    const body = await parseBody(req);
    const results = Array.isArray(body.results) ? body.results : [];
    const result = await withTransaction((ledger) => {
      const batch = ledger.batches.find((item) => item.id === batchId);
      if (!batch) return { skipCommit: true, notFound: true, data: null };
      batch.status = "completed";
      batch.completedAt = new Date().toISOString();
      batch.note = body.note ?? batch.note;
      ledger.damages.forEach((damage) => {
        if (!batch.damageIds.includes(damage.id)) return;
        const item = results.find((entry) => entry.damageId === damage.id) || {};
        damage.status = "repaired";
        damage.afterPhotoUrl = item.afterPhotoUrl || body.defaultAfterPhotoUrl || damage.afterPhotoUrl;
        damage.repairNote = item.repairNote || body.defaultRepairNote || damage.repairNote;
        damage.repairedAt = new Date().toISOString();
      });
      return { data: enrichBatch(ledger, batch) };
    });
    if (result.notFound) return send(res, 404, { error: "修补批次不存在" });
    return send(res, 200, { data: result.data });
  }

  return send(res, 404, { error: "接口不存在", routes });
}

const server = http.createServer((req, res) => {
  handle(req, res).catch((error) => send(res, error.status || 500, { error: error.message || "服务器错误" }));
});

recover()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`Rubbing repair API running at http://127.0.0.1:${PORT}`);
    });
  })
  .catch((error) => {
    console.error(`[fatal] 账目恢复失败，拒绝在旧账不明的情况下启动：${error.message}`);
    process.exit(1);
  });
