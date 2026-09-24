const http = require("http");
const fs = require("fs");
const fsp = require("fs/promises");
const path = require("path");

const PORT = Number(process.env.PORT || 3020);
const DATA_DIR = path.join(__dirname, "data");
const DB_FILE = path.join(DATA_DIR, "db.json");
const TMP_PREFIX = ".db.json.";
const TMP_SUFFIX = ".tmp";

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

/* ---------------- 持久化：原子写 + 落盘保障 ---------------- */

let tmpCounter = 0;

function fsyncDir(dir) {
  // fsync 目录确保 rename 结果落盘；个别平台不支持目录同步时忽略
  return new Promise((resolve) => {
    fs.open(dir, "r", (openErr, fd) => {
      if (openErr) return resolve();
      fs.fsync(fd, () => fs.close(fd, () => resolve()));
    });
  });
}

async function durableWriteJson(file, data) {
  const payload = JSON.stringify(data, null, 2);
  // 唯一临时文件名，保证并发/残留场景下不会撞名
  tmpCounter += 1;
  const tmpFile = path.join(
    path.dirname(file),
    `${TMP_PREFIX}${process.pid}.${tmpCounter}.${Date.now().toString(36)}${TMP_SUFFIX}`
  );

  // wx：文件必须不存在；先写临时文件再原子替换，主账永远不会出现"写了一半"的状态
  const fh = await fsp.open(tmpFile, "wx", 0o600);
  try {
    await fh.writeFile(payload, "utf8");
    await fh.sync(); // 数据与元数据落盘，成功返回后断电也不丢
  } finally {
    await fh.close();
  }
  await fsp.rename(tmpFile, file);
  await fsyncDir(path.dirname(file)); // 目录项变更落盘，rename 断电后依然生效
}

async function readJsonFile(file) {
  return JSON.parse(await fsp.readFile(file, "utf8"));
}

async function recoverStorage() {
  await fsp.mkdir(DATA_DIR, { recursive: true });

  // 上次断电可能留下未完成的临时文件：它们从未 rename，主账仍是完整旧账，直接清走
  let entries = [];
  try {
    entries = await fsp.readdir(DATA_DIR);
  } catch {
    entries = [];
  }
  await Promise.all(
    entries
      .filter((name) => name.startsWith(TMP_PREFIX) && name.endsWith(TMP_SUFFIX))
      .map((name) =>
        fsp.rm(path.join(DATA_DIR, name), { force: true }).catch(() => {})
      )
  );

  if (entries.includes(path.basename(DB_FILE))) {
    // 主账损坏时绝不静默重置，避免把旧账抹成空账
    try {
      await readJsonFile(DB_FILE);
    } catch (error) {
      throw new Error(
        `主账文件 ${DB_FILE} 无法解析，为避免覆盖旧账已停止启动，请先核查备份：${error.message}`
      );
    }
  } else {
    await durableWriteJson(DB_FILE, initialData);
  }
}

async function readDb() {
  return readJsonFile(DB_FILE);
}

/* ---------------- 写队列：所有修改事务串行执行 ---------------- */

let writeChain = Promise.resolve();

// 同时到达的修改在这里排队：每个事务独占"读最新账—修改—落盘"全过程，
// 后一个事务一定读到前一个事务的结果，不会再互相覆盖。
function withWriteTransaction(task) {
  const run = writeChain.then(() => readDb().then((db) => task(db)));
  // 队列本身不能被单个失败的事务打断
  writeChain = run.then(
    () => {},
    () => {}
  );
  return run;
}

async function commit(db) {
  await durableWriteJson(DB_FILE, db);
}

/* ---------------- HTTP 辅助 ---------------- */

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

function findRubbing(db, rubbingId) {
  const rubbing = db.rubbings.find((item) => item.id === rubbingId);
  if (!rubbing) {
    const error = new Error("拓片不存在");
    error.status = 404;
    throw error;
  }
  return rubbing;
}

function enrichBatch(db, batch) {
  const damages = db.damages.filter((item) => batch.damageIds.includes(item.id));
  return {
    ...batch,
    damages,
    total: damages.length,
    repaired: damages.filter((item) => item.status === "repaired").length,
    pending: damages.filter((item) => item.status !== "repaired").length
  };
}

/* ---------------- 路由处理（读不排队，写进事务） ---------------- */

async function handle(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = url.pathname;
  const db = await readDb();

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
    const result = await withWriteTransaction((liveDb) => {
      const rubbing = {
        id: makeId("rubbing"),
        code: body.code,
        source: body.source,
        paperSize: body.paperSize,
        note: body.note || "",
        createdAt: new Date().toISOString()
      };
      liveDb.rubbings.push(rubbing);
      return commit(liveDb).then(() => ({ status: 201, body: { data: rubbing } }));
    });
    return send(res, result.status, result.body);
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
    const result = await withWriteTransaction((liveDb) => {
      findRubbing(liveDb, rubbingId);
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
      liveDb.damages.push(damage);
      return commit(liveDb).then(() => ({ status: 201, body: { data: damage } }));
    });
    return send(res, result.status, result.body);
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
    const result = await withWriteTransaction((liveDb) => {
      const damage = liveDb.damages.find((item) => item.id === damageId);
      if (!damage) return { status: 404, body: { error: "缺损项不存在" } };
      Object.assign(damage, {
        position: body.position ?? damage.position,
        type: body.type ?? damage.type,
        beforePhotoUrl: body.beforePhotoUrl ?? damage.beforePhotoUrl,
        afterPhotoUrl: body.afterPhotoUrl ?? damage.afterPhotoUrl,
        status: body.status ?? damage.status,
        repairNote: body.repairNote ?? damage.repairNote
      });
      damage.repairedAt = damage.status === "repaired" ? new Date().toISOString() : damage.repairedAt;
      return commit(liveDb).then(() => ({ status: 200, body: { data: damage } }));
    });
    return send(res, result.status, result.body);
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
    const result = await withWriteTransaction((liveDb) => {
      // 同一笔申请里的重复项先挡下
      const duplicates = body.damageIds.filter((id, index) => body.damageIds.indexOf(id) !== index);
      if (duplicates.length) {
        return { status: 400, body: { error: `缺损项在申请中重复：${[...new Set(duplicates)].join(", ")}` } };
      }
      const invalid = body.damageIds.filter((id) => !liveDb.damages.find((damage) => damage.id === id));
      if (invalid.length) {
        return { status: 400, body: { error: `缺损项不存在：${invalid.join(", ")}` } };
      }
      // 同一处破损已被未结批次领用时，后到的申请整笔不落账，已成立的批次不受影响
      const claimed = body.damageIds
        .map((id) => {
          const damage = liveDb.damages.find((item) => item.id === id);
          const owner = liveDb.batches.find(
            (batch) => batch.status !== "completed" && batch.damageIds.includes(id)
          );
          return owner ? { damageId: id, batchId: owner.id, batchName: owner.name, position: damage.position } : null;
        })
        .filter(Boolean);
      if (claimed.length) {
        return {
          status: 409,
          body: {
            error: `以下缺损项已被未结批次领用，本笔申请未入账：${claimed
              .map((item) => `${item.position}（${item.damageId}）→ 批次 ${item.batchName}（${item.batchId}）`)
              .join("；")}`,
            conflicts: claimed
          }
        };
      }

      const batch = {
        id: makeId("batch"),
        name: body.name,
        status: "open",
        damageIds: body.damageIds,
        note: body.note || "",
        createdAt: new Date().toISOString(),
        completedAt: null
      };
      liveDb.batches.push(batch);
      liveDb.damages.forEach((damage) => {
        if (body.damageIds.includes(damage.id)) {
          damage.batchId = batch.id;
          damage.status = "in_repair";
        }
      });
      return commit(liveDb).then(() => ({ status: 201, body: { data: enrichBatch(liveDb, batch) } }));
    });
    return send(res, result.status, result.body);
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
    const result = await withWriteTransaction((liveDb) => {
      const batch = liveDb.batches.find((item) => item.id === batchId);
      if (!batch) return { status: 404, body: { error: "修补批次不存在" } };
      batch.status = "completed";
      batch.completedAt = new Date().toISOString();
      batch.note = body.note ?? batch.note;
      liveDb.damages.forEach((damage) => {
        if (!batch.damageIds.includes(damage.id)) return;
        const item = results.find((resultItem) => resultItem.damageId === damage.id) || {};
        damage.status = "repaired";
        damage.afterPhotoUrl = item.afterPhotoUrl || body.defaultAfterPhotoUrl || damage.afterPhotoUrl;
        damage.repairNote = item.repairNote || body.defaultRepairNote || damage.repairNote;
        damage.repairedAt = new Date().toISOString();
      });
      return commit(liveDb).then(() => ({ status: 200, body: { data: enrichBatch(liveDb, batch) } }));
    });
    return send(res, result.status, result.body);
  }

  return send(res, 404, { error: "接口不存在", routes });
}

const server = http.createServer((req, res) => {
  handle(req, res).catch((error) => send(res, error.status || 500, { error: error.message || "服务器错误" }));
});

recoverStorage()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`Rubbing repair API running at http://127.0.0.1:${PORT}`);
    });
  })
  .catch((error) => {
    console.error(error.message);
    process.exit(1);
  });
