// 端到端验证：并发保存、领用冲突、重启可查、断电恢复
// 用法：node test/stability.test.js
const { spawn } = require("child_process");
const fs = require("fs");
const path = require("path");
const os = require("os");
const assert = require("assert");

const ROOT = path.join(__dirname, "..");
const TMP_HOME = fs.mkdtempSync(path.join(os.tmpdir(), "rubbing-db-"));
const DATA_DIR = path.join(TMP_HOME, "data");
fs.mkdirSync(DATA_DIR, { recursive: true });
const PORT = 3399;
const BASE = `http://127.0.0.1:${PORT}`;

let server = null;

// server.js 以 __dirname 定位 data，测试复制一份服务器代码并把 data 目录替换为临时目录
function materializeServer() {
  const code = fs
    .readFileSync(path.join(ROOT, "server.js"), "utf8")
    .replace('path.join(__dirname, "data")', JSON.stringify(DATA_DIR));
  const entry = path.join(TMP_HOME, "server.js");
  fs.writeFileSync(entry, code);
  return entry;
}
const ENTRY = materializeServer();

function launch() {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [ENTRY], {
      env: { ...process.env, PORT: String(PORT) }
    });
    let settled = false;
    child.stdout.on("data", (chunk) => {
      if (!settled && String(chunk).includes("running")) {
        settled = true;
        resolve(child);
      }
    });
    child.on("exit", (code) => {
      if (!settled) reject(new Error("server exited before ready: " + code));
    });
    setTimeout(() => {
      if (!settled) reject(new Error("server start timeout"));
    }, 5000);
  });
}

async function waitDown(child) {
  if (!child || child.killed) return;
  child.kill("SIGKILL");
  await new Promise((r) => child.on("exit", r));
}

async function api(method, urlPath, body) {
  const res = await fetch(BASE + urlPath, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined
  });
  const json = await res.json();
  return { status: res.status, json };
}

async function waitHealthy() {
  for (let i = 0; i < 50; i++) {
    try {
      const r = await fetch(BASE + "/health");
      if (r.ok) return;
    } catch {}
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error("server not healthy");
}

function rawDb() {
  return JSON.parse(fs.readFileSync(path.join(DATA_DIR, "db.json"), "utf8"));
}

async function main() {
  // 1) 启动 + 初始库
  server = await launch();
  await waitHealthy();

  // 2) 两位师傅几乎同时各建一个批次（不同破损），两笔都必须保留
  const r1Promise = api("POST", "/batches", { name: "王师傅批次", damageIds: ["damage_demo_1"] });
  const r2Promise = api("POST", "/batches", { name: "李师傅批次", damageIds: ["damage_demo_2"] });
  const [r1, r2] = await Promise.all([r1Promise, r2Promise]);
  assert.strictEqual(r1.status, 201, `批次1应成功，实际 ${r1.status} ${JSON.stringify(r1.json)}`);
  assert.strictEqual(r2.status, 201, `批次2应成功，实际 ${r2.status} ${JSON.stringify(r2.json)}`);

  const list = await api("GET", "/batches");
  assert.strictEqual(list.json.data.length, 2, `两个并发批次都应在账，实际 ${list.json.data.length}`);
  const names = list.json.data.map((b) => b.name).sort();
  assert.deepStrictEqual(names, ["李师傅批次", "王师傅批次"]);
  console.log("✓ 并发保存：两笔均落账，互不覆盖");

  // 3) 同一处破损被两个未结批次领用：后到的拒绝，先成立的不动
  const conflict = await api("POST", "/batches", {
    name: "争抢批次",
    damageIds: ["damage_demo_1", "damage_demo_2"]
  });
  assert.strictEqual(conflict.status, 409, `冲突应返回409，实际 ${conflict.status}`);
  assert.strictEqual(conflict.json.conflicts.length, 2, "应报告两处冲突");
  const afterConflict = rawDb();
  assert.strictEqual(afterConflict.batches.length, 2, "冲突申请不落账，批次数量不变");
  assert.ok(
    afterConflict.damages.every((d) => d.batchId !== null && d.status === "in_repair"),
    "已领用破损归属不受影响"
  );
  console.log("✓ 领用冲突：后到申请被拒(409)且不落账，既有批次无损");

  // 4) 同一破损的两个领用请求同时到达：恰好一个成功，一个 409
  const extraDamage = await api("POST", "/rubbings/rubbing_demo/damages", {
    position: "背面右上角",
    type: "霉斑",
    beforePhotoUrl: "https://example.local/before-3.jpg"
  });
  const d3 = extraDamage.json.data.id;
  const c1p = api("POST", "/batches", { name: "并发争抢A", damageIds: [d3] });
  const c2p = api("POST", "/batches", { name: "并发争抢B", damageIds: [d3] });
  const [c1, c2] = await Promise.all([c1p, c2p]);
  const statuses = [c1.status, c2.status].sort();
  assert.deepStrictEqual(statuses, [201, 409], `同破损并发应为一成一拒，实际 ${statuses}`);
  assert.strictEqual(rawDb().batches.length, 3, "只有一笔争抢批次入账");
  console.log("✓ 并发冲突：同一破损同时被领用时恰有一笔成立");

  // 5) 批次完工后，破损可被新批次领用（只拦未结批次）
  const firstBatchId = r1.json.data.id;
  const done = await api("POST", `/batches/${firstBatchId}/complete`, {
    defaultAfterPhotoUrl: "https://example.local/after.jpg"
  });
  assert.strictEqual(done.status, 200);
  const reopen = await api("POST", "/batches", { name: "返工批次", damageIds: ["damage_demo_1"] });
  assert.strictEqual(reopen.status, 201, `完工后应可重新领用，实际 ${reopen.status} ${JSON.stringify(reopen.json)}`);
  console.log("✓ 已结批次释放破损，可再次领用");

  // 6) 重启（杀掉再启动）：成功记录仍可查
  await waitDown(server);
  server = null;
  const onDiskBefore = rawDb();
  server = await launch();
  await waitHealthy();
  const afterReboot = await api("GET", "/batches");
  assert.strictEqual(afterReboot.json.data.length, 4, "四笔成功批次全部可查");
  assert.deepStrictEqual(rawDb(), onDiskBefore, "重启前后账目一致");
  console.log("✓ 重启持久化：已成功记录全部可查");

  // 7) 模拟断电：遗留半截临时文件 + 主账完整 → 重启清走残留，旧账完整打开
  fs.writeFileSync(path.join(DATA_DIR, ".db.json.fake.999.1.abc.tmp"), '{"half-written": true');
  await waitDown(server);
  server = null;
  server = await launch();
  await waitHealthy();
  const leftover = fs.readdirSync(DATA_DIR).filter((n) => n.endsWith(".tmp"));
  assert.strictEqual(leftover.length, 0, "断电残留临时文件应在启动时清走");
  assert.deepStrictEqual(rawDb(), onDiskBefore, "恢复后旧账完整");
  const stillThere = await api("GET", `/batches/${firstBatchId}`);
  assert.strictEqual(stillThere.status, 200);
  assert.strictEqual(stillThere.json.data.status, "completed");
  console.log("✓ 断电恢复：残留临时写入被清理，完整旧账照常打开");

  // 8) 主账损坏时拒绝启动（不静默重置成空账）
  await waitDown(server);
  server = null;
  fs.writeFileSync(path.join(DATA_DIR, "db.json"), '{ "batches": [ BROKEN');
  const bad = spawn(process.execPath, [ENTRY], { env: { ...process.env, PORT: String(PORT) } });
  let stderrText = "";
  bad.stderr.on("data", (chunk) => (stderrText += chunk));
  const exitCode = await new Promise((resolve) => bad.on("exit", resolve));
  assert.notStrictEqual(exitCode, 0, "主账损坏时应非零退出");
  assert.ok(stderrText.includes("无法解析"), "应提示无法解析而非覆盖旧账");
  assert.ok(
    fs.readFileSync(path.join(DATA_DIR, "db.json"), "utf8").includes("BROKEN"),
    "损坏的旧账文件必须原样保留"
  );
  console.log("✓ 旧账损坏保护：拒绝启动并保留现场，不用空账覆盖");

  console.log("\n全部验证通过 ✅");
}

main()
  .catch((err) => {
    console.error("验证失败：", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await waitDown(server);
  });
