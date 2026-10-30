/**
 * 楚新钓联机中继：世界聊天 + 房间邀请 + 账号/云存档
 * 启动：npm start   端口：8787
 */
const http = require("http");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8787;
const DATA_DIR = path.join(__dirname, "data");
const ACCOUNTS_FILE = path.join(DATA_DIR, "accounts.json");
const TOKENS_FILE = path.join(DATA_DIR, "tokens.json");
const SAVES_DIR = path.join(DATA_DIR, "saves");

/** @type {Map<string, any>} */
const clients = new Map();
/** @type {Map<string, {code:string, hostId:string, members:Set<string>}>} */
const rooms = new Map();
/** @type {Map<string, string>} */
const codes = new Map();
/** @type {Map<string, string>} token -> username */
const tokens = new Map();

function ensureData() {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(SAVES_DIR)) fs.mkdirSync(SAVES_DIR, { recursive: true });
  if (!fs.existsSync(ACCOUNTS_FILE)) fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify({ users: {} }, null, 2));
  if (!fs.existsSync(TOKENS_FILE)) fs.writeFileSync(TOKENS_FILE, JSON.stringify({ tokens: {} }, null, 2));
}

function loadTokens() {
  ensureData();
  try {
    const db = JSON.parse(fs.readFileSync(TOKENS_FILE, "utf8"));
    const map = db && typeof db.tokens === "object" ? db.tokens : {};
    tokens.clear();
    for (const [tok, user] of Object.entries(map)) {
      if (tok && user) tokens.set(String(tok), String(user));
    }
  } catch {
    /* keep empty */
  }
}

function persistTokens() {
  ensureData();
  const obj = {};
  for (const [tok, user] of tokens) obj[tok] = user;
  fs.writeFileSync(TOKENS_FILE, JSON.stringify({ tokens: obj }, null, 2));
}

function issueToken(user) {
  const token = uid() + uid();
  tokens.set(token, user);
  persistTokens();
  return token;
}

function loadAccounts() {
  ensureData();
  try {
    return JSON.parse(fs.readFileSync(ACCOUNTS_FILE, "utf8"));
  } catch {
    return { users: {} };
  }
}

function saveAccounts(db) {
  ensureData();
  fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(db, null, 2));
}

function hashPass(password, salt) {
  return crypto.createHash("sha256").update(`${salt}|${password}`).digest("hex");
}

function validUser(user) {
  user = String(user || "").trim().toLowerCase();
  if (user.length < 2 || user.length > 16) return "";
  if (!/^[a-z0-9_]+$/.test(user)) return "";
  return user;
}

function savePath(user) {
  return path.join(SAVES_DIR, `${user}.json`);
}

function readSave(user) {
  const p = savePath(user);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function writeSave(user, data) {
  ensureData();
  fs.writeFileSync(savePath(user), JSON.stringify(data));
}

function uid() {
  return crypto.randomBytes(6).toString("hex");
}

function makeCode() {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  let c = "";
  for (let i = 0; i < 5; i++) c += alphabet[Math.floor(Math.random() * alphabet.length)];
  if (codes.has(c)) return makeCode();
  return c;
}

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function broadcastWorld(obj) {
  for (const [, c] of clients) send(c.ws, obj);
}

function clientPublic(c) {
  return {
    id: c.id,
    name: c.name || "钓客",
    spot: c.spot || "",
    fishing: !!c.fishing,
    phase: c.fishPhase || (c.fishing ? "fish" : "hub"),
    title: c.title || "",
  };
}

/** 踢掉同账号 / 同昵称的旧连接，避免重连或开多页出现两个自己 */
function kickDuplicateIdentity(exceptId, { name = "", user = "" } = {}) {
  const nm = String(name || "").trim();
  const u = String(user || "").trim();
  if ((!nm || nm === "钓客") && !u) return;
  const doomed = [];
  for (const [cid, c] of clients) {
    if (cid === exceptId) continue;
    const sameUser = u && String(c.user || "") === u;
    const sameName = nm && nm !== "钓客" && String(c.name || "") === nm;
    if (sameUser || sameName) doomed.push(cid);
  }
  for (const cid of doomed) {
    const c = clients.get(cid);
    if (!c) continue;
    const spot = c.spot;
    const wasRoom = !!c.roomId;
    try {
      leaveRoom(c);
    } catch (_) {}
    clients.delete(cid);
    try {
      c.ws.close();
    } catch (_) {}
    if (!wasRoom) emitSpotPresence(spot);
  }
}

function roomMembers(room) {
  const list = [];
  for (const id of room.members) {
    const c = clients.get(id);
    if (!c) continue;
    list.push(clientPublic(c));
  }
  return list;
}

/** 未进房间时：同钓场的人也能互相看见 */
function spotMembers(spot) {
  const s = String(spot || "");
  const list = [];
  for (const c of clients.values()) {
    if (c.roomId) continue; // 已在房间的人只走房间同步，避免串台
    if (String(c.spot || "") !== s) continue;
    list.push(clientPublic(c));
  }
  return list;
}

function emitSpotPresence(spot) {
  const s = String(spot || "");
  if (!s) return;
  const payload = {
    t: "presence",
    world_spot: s,
    members: spotMembers(s),
  };
  for (const c of clients.values()) {
    if (c.roomId) continue;
    if (String(c.spot || "") !== s) continue;
    send(c.ws, payload);
  }
}

function emitRoom(room) {
  const payload = {
    t: "room",
    room: room.id,
    code: room.code,
    world_spot: room.worldSpot || "stream",
    members: roomMembers(room),
  };
  for (const id of room.members) {
    const c = clients.get(id);
    if (!c) continue;
    send(c.ws, { ...payload, host: id === room.hostId });
  }
}

function leaveRoom(client) {
  if (!client.roomId) return;
  const room = rooms.get(client.roomId);
  client.roomId = null;
  if (!room) return;
  room.members.delete(client.id);
  if (room.members.size === 0) {
    codes.delete(room.code);
    rooms.delete(room.id);
    emitSpotPresence(client.spot);
    return;
  }
  if (room.hostId === client.id) {
    room.hostId = [...room.members][0];
  }
  emitRoom(room);
  for (const id of room.members) {
    const c = clients.get(id);
    if (c) send(c.ws, { t: "sys", text: `${client.name} 离开了房间` });
  }
}

function cloudRegister(userRaw, password) {
  const user = validUser(userRaw);
  const pwd = String(password || "");
  if (!user) return { ok: false, error: "账号需 2–16 位，仅字母数字下划线" };
  if (pwd.length < 4) return { ok: false, error: "密码至少 4 位" };
  const db = loadAccounts();
  if (db.users[user]) return { ok: false, error: "云端账号已存在，请直接登录" };
  const salt = crypto.randomBytes(8).toString("hex");
  db.users[user] = { salt, hash: hashPass(pwd, salt), created: Date.now() };
  saveAccounts(db);
  const token = issueToken(user);
  return { ok: true, user, token, save: null };
}

function cloudLogin(userRaw, password) {
  const user = validUser(userRaw);
  const pwd = String(password || "");
  const db = loadAccounts();
  if (!user || !db.users[user]) return { ok: false, error: "云端账号不存在" };
  const info = db.users[user];
  if (hashPass(pwd, info.salt) !== info.hash) return { ok: false, error: "云端密码错误" };
  const token = issueToken(user);
  return { ok: true, user, token, save: readSave(user) };
}

function userForToken(token) {
  const user = tokens.get(String(token || ""));
  return user || "";
}

function weekIdOf(raw) {
  const w = String(raw || "").trim();
  if (/^w\d+$/.test(w)) return w;
  return `w${Math.floor(Date.now() / 1000 / 604800)}`;
}

function rankPath(weekId) {
  return path.join(DATA_DIR, `endless_rank_${weekId}.json`);
}

function readRank(weekId) {
  ensureData();
  const p = rankPath(weekId);
  if (!fs.existsSync(p)) return [];
  try {
    const list = JSON.parse(fs.readFileSync(p, "utf8"));
    return Array.isArray(list) ? list : [];
  } catch {
    return [];
  }
}

function writeRank(weekId, list) {
  ensureData();
  fs.writeFileSync(rankPath(weekId), JSON.stringify(list, null, 2));
}

function sortRank(list) {
  return list
    .filter((e) => e && typeof e === "object" && e.user)
    .sort((a, b) => {
      const df = (Number(b.floor) || 0) - (Number(a.floor) || 0);
      if (df !== 0) return df;
      return (Number(a.at) || 0) - (Number(b.at) || 0);
    })
    .slice(0, 50);
}

function endlessRankSubmit(user, data) {
  const weekId = weekIdOf(data.week);
  const floor = Math.max(0, Math.min(99999, Math.floor(Number(data.floor) || 0)));
  if (floor <= 0) return { ok: false, error: "层数无效" };
  let list = readRank(weekId);
  const now = Date.now();
  const idx = list.findIndex((e) => String(e.user) === user);
  if (idx >= 0) {
    const old = Number(list[idx].floor) || 0;
    if (floor > old) list[idx] = { user, floor, at: now };
  } else {
    list.push({ user, floor, at: now });
  }
  list = sortRank(list);
  writeRank(weekId, list);
  const rank = list.findIndex((e) => String(e.user) === user) + 1;
  return { ok: true, week: weekId, floor, rank, total: list.length };
}

function endlessRankList(user, data) {
  const weekId = weekIdOf(data.week);
  const list = sortRank(readRank(weekId));
  const top = list.slice(0, 20).map((e, i) => ({
    rank: i + 1,
    user: String(e.user),
    floor: Number(e.floor) || 0,
  }));
  let self = null;
  const si = list.findIndex((e) => String(e.user) === user);
  if (si >= 0) self = { rank: si + 1, user, floor: Number(list[si].floor) || 0 };
  return { ok: true, week: weekId, top, self, total: list.length };
}

function dayIdOf(raw) {
  const d = String(raw || "").trim();
  if (/^\d{8}$/.test(d)) return d;
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const day = String(now.getDate()).padStart(2, "0");
  return `${y}${m}${day}`;
}

function limitedRankPath(day, fishId) {
  const fid = String(fishId || "all").replace(/[^a-z0-9_]/gi, "").slice(0, 40) || "all";
  return path.join(DATA_DIR, `daily_limited_rank_${day}_${fid}.json`);
}

function readLimitedRank(day, fishId) {
  ensureData();
  const p = limitedRankPath(day, fishId);
  if (!fs.existsSync(p)) return [];
  try {
    const list = JSON.parse(fs.readFileSync(p, "utf8"));
    return Array.isArray(list) ? list : [];
  } catch {
    return [];
  }
}

function writeLimitedRank(day, fishId, list) {
  ensureData();
  fs.writeFileSync(limitedRankPath(day, fishId), JSON.stringify(list, null, 2));
}

function sortLimitedRank(list) {
  return list
    .filter((e) => e && typeof e === "object" && e.user)
    .sort((a, b) => {
      const dt = (Number(a.time) || 99999) - (Number(b.time) || 99999);
      if (Math.abs(dt) > 0.001) return dt;
      return (Number(a.at) || 0) - (Number(b.at) || 0);
    })
    .slice(0, 50);
}

function dailyLimitedRankSubmit(user, data) {
  const day = dayIdOf(data.day);
  const fishId = String(data.fish_id || "").trim().slice(0, 40);
  if (!fishId) return { ok: false, error: "鱼种无效" };
  const time = Math.max(0.1, Math.min(3600, Number(data.time) || 0));
  const grade = String(data.grade || "").slice(0, 8);
  let list = readLimitedRank(day, fishId);
  const now = Date.now();
  const idx = list.findIndex((e) => String(e.user) === user);
  if (idx >= 0) {
    const old = Number(list[idx].time) || 99999;
    if (time < old - 0.01) list[idx] = { user, time, grade, at: now };
  } else {
    list.push({ user, time, grade, at: now });
  }
  list = sortLimitedRank(list);
  writeLimitedRank(day, fishId, list);
  const rank = list.findIndex((e) => String(e.user) === user) + 1;
  return { ok: true, day, fish_id: fishId, time, grade, rank, total: list.length };
}

function dailyLimitedRankList(user, data) {
  const day = dayIdOf(data.day);
  const fishId = String(data.fish_id || "").trim().slice(0, 40);
  if (!fishId) return { ok: false, error: "请指定限定 Boss" };
  const list = sortLimitedRank(readLimitedRank(day, fishId));
  const top = list.slice(0, 20).map((e, i) => ({
    rank: i + 1,
    user: String(e.user),
    time: Number(e.time) || 0,
    grade: String(e.grade || ""),
  }));
  let self = null;
  const si = list.findIndex((e) => String(e.user) === user);
  if (si >= 0) {
    self = {
      rank: si + 1,
      user,
      time: Number(list[si].time) || 0,
      grade: String(list[si].grade || ""),
    };
  }
  return { ok: true, day, fish_id: fishId, top, self, total: list.length };
}

function handleAuthRegister(ws, client, data) {
  const result = cloudRegister(data.user, data.password);
  if (!result.ok) {
    send(ws, { t: "auth_fail", text: result.error });
    return;
  }
  client.user = result.user;
  client.name = result.user;
  client.token = result.token;
  kickDuplicateIdentity(client.id, { name: result.user, user: result.user });
  send(ws, { t: "auth_ok", user: result.user, token: result.token, save: null });
  send(ws, { t: "sys", text: `云账号 ${result.user} 注册成功（纯新档）` });
}

function handleAuthLogin(ws, client, data) {
  const result = cloudLogin(data.user, data.password);
  if (!result.ok) {
    send(ws, { t: "auth_fail", text: result.error });
    return;
  }
  client.user = result.user;
  client.name = result.user;
  client.token = result.token;
  kickDuplicateIdentity(client.id, { name: result.user, user: result.user });
  send(ws, { t: "auth_ok", user: result.user, token: result.token, save: result.save });
  send(ws, { t: "sys", text: `云账号 ${result.user} 已登录` });
}

function handleAuthResume(ws, client, data) {
  const token = String(data.token || "");
  const user = tokens.get(token);
  if (!user) {
    send(ws, { t: "auth_fail", text: "云会话已失效，请重新登录" });
    return;
  }
  client.user = user;
  client.name = user;
  client.token = token;
  kickDuplicateIdentity(client.id, { name: user, user });
  send(ws, { t: "auth_ok", user, token, save: readSave(user) });
  send(ws, { t: "sys", text: `云账号 ${user} 已恢复会话` });
}

ensureData();
loadTokens();

/** 静态站：优先 server/public，其次上级 site/ */
const PUBLIC_DIRS = [
  path.join(__dirname, "public"),
  path.join(__dirname, "..", "site"),
].filter((d) => fs.existsSync(d));

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".mjs": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webp": "image/webp",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".wasm": "application/wasm",
  ".pck": "application/octet-stream",
  ".ogg": "audio/ogg",
  ".mp3": "audio/mpeg",
  ".wav": "audio/wav",
};

function safeJoin(root, reqPath) {
  const decoded = decodeURIComponent((reqPath || "/").split("?")[0]);
  const cleaned = path.normalize(decoded).replace(/^(\.\.[/\\])+/, "");
  const full = path.join(root, cleaned);
  if (!full.startsWith(root)) return null;
  return full;
}

function sendFile(req, res, filePath) {
  const ext = path.extname(filePath).toLowerCase();
  const type = MIME[ext] || "application/octet-stream";
  let servePath = filePath;
  let encoding = null;
  const accept = String(req.headers["accept-encoding"] || "");
  // 预压缩：wasm/js 可大幅缩小；pck 几乎压不动，跳过
  if (/\bgzip\b/.test(accept) && [".wasm", ".js", ".html", ".css", ".json", ".svg"].includes(ext)) {
    const gz = `${filePath}.gz`;
    if (fs.existsSync(gz)) {
      servePath = gz;
      encoding = "gzip";
    }
  }
  const stat = fs.statSync(servePath);
  const size = stat.size;
  const etag = `"${stat.mtimeMs.toString(16)}-${size.toString(16)}${encoding ? "-gz" : ""}"`;
  const lastMod = stat.mtime.toUTCString();
  const heavy = [".pck", ".wasm", ".js", ".png", ".jpg", ".jpeg", ".webp", ".ogg", ".mp3", ".wav", ".ico"].includes(ext);
  const base = {
    "Content-Type": type,
    "Accept-Ranges": encoding ? "none" : "bytes",
    ETag: etag,
    "Last-Modified": lastMod,
    // html 不缓存；大资源用 ETag：本地已有则 304 不重下，文件更新后自动换新
    "Cache-Control":
      ext === ".html" ? "no-cache" : heavy ? "public, max-age=0, must-revalidate" : "public, max-age=3600",
    "Cross-Origin-Resource-Policy": "same-origin",
    Vary: "Accept-Encoding",
  };
  if (encoding) base["Content-Encoding"] = encoding;

  // 已有完整缓存且无 Range：直接 304
  const inm = req.headers["if-none-match"];
  const ims = req.headers["if-modified-since"];
  if (!req.headers.range && (inm === etag || (ims && Date.parse(ims) >= stat.mtime.getTime()))) {
    res.writeHead(304, base);
    res.end();
    return;
  }

  // gzip 实体不做 Range（长度与源文件不同）
  const range = encoding ? null : req.headers.range;
  if (range && /^bytes=/.test(range)) {
    const m = /^bytes=(\d*)-(\d*)$/.exec(range);
    if (m) {
      let start = m[1] === "" ? 0 : parseInt(m[1], 10);
      let end = m[2] === "" ? size - 1 : parseInt(m[2], 10);
      if (Number.isNaN(start) || Number.isNaN(end) || start > end || start >= size) {
        res.writeHead(416, { "Content-Range": `bytes */${size}`, ETag: etag });
        res.end();
        return;
      }
      end = Math.min(end, size - 1);
      const chunk = end - start + 1;
      res.writeHead(206, {
        ...base,
        "Content-Range": `bytes ${start}-${end}/${size}`,
        "Content-Length": chunk,
      });
      fs.createReadStream(servePath, { start, end }).pipe(res);
      return;
    }
  }

  res.writeHead(200, {
    ...base,
    "Content-Length": size,
  });
  fs.createReadStream(servePath).pipe(res);
}

function tryStatic(req, res) {
  if (PUBLIC_DIRS.length === 0) return false;
  let urlPath = (req.url || "/").split("?")[0];
  if (urlPath === "/") urlPath = "/index.html";
  for (const root of PUBLIC_DIRS) {
    let filePath = safeJoin(root, urlPath);
    if (!filePath) continue;
    if (fs.existsSync(filePath) && fs.statSync(filePath).isDirectory()) {
      filePath = path.join(filePath, "index.html");
    }
    if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
      sendFile(req, res, filePath);
      return true;
    }
  }
  return false;
}

function sendJson(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on("data", (c) => {
      chunks.push(c);
      if (Buffer.concat(chunks).length > 2_000_000) req.destroy();
    });
    req.on("end", () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"));
      } catch {
        resolve({});
      }
    });
    req.on("error", () => resolve({}));
  });
}

const server = http.createServer(async (req, res) => {
  const urlPath = (req.url || "/").split("?")[0];
  if (urlPath.startsWith("/api/cloud")) {
    if (req.method === "OPTIONS") {
      sendJson(res, 204, {});
      return;
    }
    if (urlPath === "/api/cloud/health") {
      sendJson(res, 200, { ok: true });
      return;
    }
    if (req.method !== "POST") {
      sendJson(res, 405, { ok: false, error: "请用 POST" });
      return;
    }
    const data = await readBody(req);
    if (urlPath === "/api/cloud/register") {
      sendJson(res, 200, cloudRegister(data.user, data.password));
      return;
    }
    if (urlPath === "/api/cloud/login") {
      sendJson(res, 200, cloudLogin(data.user, data.password));
      return;
    }
    const user = userForToken(data.token);
    if (!user) {
      sendJson(res, 200, { ok: false, error: "云会话已失效，请重新登录" });
      return;
    }
    if (urlPath === "/api/cloud/push") {
      if (!data.save || typeof data.save !== "object" || Array.isArray(data.save)) {
        sendJson(res, 200, { ok: false, error: "存档无效" });
        return;
      }
      writeSave(user, data.save);
      sendJson(res, 200, { ok: true });
      return;
    }
    if (urlPath === "/api/cloud/pull") {
      sendJson(res, 200, { ok: true, save: readSave(user) || {} });
      return;
    }
    if (urlPath === "/api/cloud/delete") {
      const p = savePath(user);
      if (fs.existsSync(p)) fs.unlinkSync(p);
      sendJson(res, 200, { ok: true });
      return;
    }
    if (urlPath === "/api/cloud/endless_rank_submit") {
      sendJson(res, 200, endlessRankSubmit(user, data));
      return;
    }
    if (urlPath === "/api/cloud/endless_rank_list") {
      sendJson(res, 200, endlessRankList(user, data));
      return;
    }
    if (urlPath === "/api/cloud/daily_limited_rank_submit") {
      sendJson(res, 200, dailyLimitedRankSubmit(user, data));
      return;
    }
    if (urlPath === "/api/cloud/daily_limited_rank_list") {
      sendJson(res, 200, dailyLimitedRankList(user, data));
      return;
    }
    sendJson(res, 404, { ok: false, error: "未知接口" });
    return;
  }
  if (req.url === "/health" || req.url === "/health/") {
    res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
    res.end(JSON.stringify({ ok: true, clients: clients.size, rooms: rooms.size }));
    return;
  }
  if (tryStatic(req, res)) return;
  res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8" });
  res.end("楚新钓联机服务运行中。打开 / 进入游戏，WebSocket 路径 /ws\n");
});

// 网页版 .pck/.wasm 较大，经隧道下载需更长超时
server.timeout = 0;
server.requestTimeout = 0;
server.headersTimeout = 0;
server.keepAliveTimeout = 120000;

const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", (ws) => {
  const id = uid();
  const client = {
    id,
    ws,
    name: "钓客",
    spot: "stream",
    fishing: false,
    fishPhase: "hub",
    title: "",
    roomId: null,
    user: "",
    token: "",
  };
  clients.set(id, client);
  send(ws, { t: "welcome", id });
  send(ws, { t: "sys", text: "欢迎来到楚新钓世界频道" });

  ws.on("message", (buf) => {
    let data;
    try {
      data = JSON.parse(String(buf));
    } catch {
      return;
    }
    const t = data.t;
    if (t === "hello") {
      client.name = String(data.name || "钓客").slice(0, 12);
      // 网页云存档不走联机 auth：用昵称（账号名）踢掉重连残影 / 多开页
      kickDuplicateIdentity(id, { name: client.name, user: client.user || "" });
      return;
    }
    if (t === "auth_register") {
      handleAuthRegister(ws, client, data);
      return;
    }
    if (t === "auth_login") {
      handleAuthLogin(ws, client, data);
      return;
    }
    if (t === "auth_resume") {
      handleAuthResume(ws, client, data);
      return;
    }
    if (t === "save_push") {
      const user = validUser(data.user);
      const token = String(data.token || "");
      if (!user || tokens.get(token) !== user) {
        send(ws, { t: "error", text: "存档上传未授权" });
        return;
      }
      if (data.save && typeof data.save === "object") {
        writeSave(user, data.save);
        send(ws, { t: "save_ok" });
      }
      return;
    }
    if (t === "save_pull") {
      const user = validUser(data.user);
      const token = String(data.token || "");
      if (!user || tokens.get(token) !== user) {
        send(ws, { t: "error", text: "存档拉取未授权" });
        return;
      }
      send(ws, { t: "save_data", save: readSave(user) || {} });
      return;
    }
    if (t === "save_delete") {
      const user = validUser(data.user);
      const token = String(data.token || "");
      if (!user || tokens.get(token) !== user) {
        send(ws, { t: "error", text: "删除存档未授权" });
        return;
      }
      const p = savePath(user);
      if (fs.existsSync(p)) fs.unlinkSync(p);
      send(ws, { t: "save_ok" });
      send(ws, { t: "sys", text: "云存档已删除" });
      return;
    }
    if (t === "chat") {
      const ch = data.ch === "room" ? "room" : "world";
      const text = String(data.text || "").slice(0, 80);
      if (!text) return;
      const name = String(data.name || client.name).slice(0, 12);
      client.name = name;
      const msg = { t: "chat", ch, from: name, text };
      if (ch === "world") {
        broadcastWorld(msg);
      } else if (client.roomId) {
        const room = rooms.get(client.roomId);
        if (!room) return;
        for (const mid of room.members) {
          const c = clients.get(mid);
          if (c) send(c.ws, msg);
        }
      }
      return;
    }
    if (t === "create") {
      leaveRoom(client);
      client.name = String(data.name || client.name).slice(0, 12);
      client.spot = String(data.spot || "stream");
      client.fishing = false;
      client.fishPhase = "hub";
      const roomId = uid();
      const code = makeCode();
      const room = {
        id: roomId,
        code,
        hostId: id,
        members: new Set([id]),
        worldSpot: client.spot,
      };
      rooms.set(roomId, room);
      codes.set(code, roomId);
      client.roomId = roomId;
      emitRoom(room);
      send(ws, { t: "sys", text: `世界已创建，房间码 ${code}（同地图可一起钓鱼）` });
      return;
    }
    if (t === "join") {
      const code = String(data.code || "").toUpperCase();
      const roomId = codes.get(code);
      if (!roomId) {
        send(ws, { t: "error", text: "房间码无效" });
        return;
      }
      const room = rooms.get(roomId);
      if (!room) {
        send(ws, { t: "error", text: "房间已解散" });
        return;
      }
      leaveRoom(client);
      client.name = String(data.name || client.name).slice(0, 12);
      // 加入后进入同一世界地图
      client.spot = String(room.worldSpot || data.spot || "stream");
      client.fishing = false;
      client.fishPhase = "hub";
      client.roomId = roomId;
      room.members.add(id);
      emitRoom(room);
      for (const mid of room.members) {
        const c = clients.get(mid);
        if (c && mid !== id) send(c.ws, { t: "sys", text: `${client.name} 加入了你的世界` });
      }
      send(ws, {
        t: "sys",
        text: `已加入房间 ${code} · 世界地图：${client.spot}（可一起出钓）`,
      });
      return;
    }
    if (t === "leave") {
      leaveRoom(client);
      send(ws, { t: "room", room: "", code: "", members: [], host: false });
      emitSpotPresence(client.spot);
      return;
    }
    if (t === "presence") {
      const prevSpot = client.spot;
      client.name = String(data.name || client.name).slice(0, 12);
      client.spot = String(data.spot || client.spot);
      client.fishing = !!data.fishing;
      if (data.title != null) client.title = String(data.title).slice(0, 24);
      if (data.phase != null) client.fishPhase = String(data.phase).slice(0, 16);
      else client.fishPhase = client.fishing ? "fish" : "hub";
      if (client.roomId) {
        const room = rooms.get(client.roomId);
        if (room) {
          // 房主切换钓场 = 世界地图
          if (room.hostId === id) {
            room.worldSpot = client.spot;
          }
          const payload = {
            t: "presence",
            world_spot: room.worldSpot || client.spot,
            members: roomMembers(room),
          };
          for (const mid of room.members) {
            const c = clients.get(mid);
            if (c) send(c.ws, payload);
          }
        }
      } else {
        // 换图时刷新旧图名单
        if (prevSpot && prevSpot !== client.spot) emitSpotPresence(prevSpot);
        emitSpotPresence(client.spot);
      }
      return;
    }
    if (t === "fish") {
      // 同地图互见：优先房间；无房间则同钓场世界频道
      client.name = String(data.name || client.name).slice(0, 12);
      client.spot = String(data.spot || client.spot);
      client.fishing = true;
      client.fishPhase = String(data.phase || "fish").slice(0, 16);
      const payload = {
        t: "fish",
        from: id,
        name: client.name,
        spot: client.spot,
        phase: client.fishPhase,
        bx: Number(data.bx) || 0,
        bz: Number(data.bz) || 0,
      };
      // 技能演出
      if (data.sk) payload.sk = String(data.sk).slice(0, 32);
      if (data.style) payload.style = String(data.style).slice(0, 16);
      if (data.kit) payload.kit = String(data.kit).slice(0, 16);
      if (data.sn) payload.sn = String(data.sn).slice(0, 24);
      if (data.tier != null) payload.tier = Math.min(5, Math.max(-1, Number(data.tier) || 4));
      if (data.vr != null) payload.vr = Math.min(3, Math.max(1, Number(data.vr) || 1));
      if (data.cr != null) payload.cr = Number(data.cr) || 0;
      if (data.cg != null) payload.cg = Number(data.cg) || 0;
      if (data.cb != null) payload.cb = Number(data.cb) || 0;
      // 同伴鱼 / 血量 / 坐标
      if (data.fid) payload.fid = String(data.fid).slice(0, 40);
      if (data.fn) payload.fn = String(data.fn).slice(0, 24);
      if (data.boss != null) payload.boss = Number(data.boss) ? 1 : 0;
      if (data.fw != null) payload.fw = Math.min(500, Math.max(0, Number(data.fw) || 0));
      if (data.fhp != null) payload.fhp = Math.max(0, Number(data.fhp) || 0);
      if (data.fmx != null) payload.fmx = Math.max(0, Number(data.fmx) || 0);
      if (data.lhp != null) payload.lhp = Math.max(0, Number(data.lhp) || 0);
      if (data.lmx != null) payload.lmx = Math.max(0, Number(data.lmx) || 0);
      if (data.fx != null) payload.fx = Number(data.fx) || 0;
      if (data.fy != null) payload.fy = Number(data.fy) || 0;
      if (data.fz != null) payload.fz = Number(data.fz) || 0;
      // 矛鱼：同 Boss 共斗
      if (data.mao != null) payload.mao = Math.min(9, Math.max(0, Number(data.mao) || 0));
      if (data.sid) payload.sid = String(data.sid).slice(0, 48);
      if (data.blv != null) payload.blv = Math.max(0, Math.min(20, Number(data.blv) || 0));
      if (data.blm != null) payload.blm = Math.max(0, Math.min(20, Number(data.blm) || 0));
      if (data.arm != null) payload.arm = String(data.arm).slice(0, 16);
      if (data.ok != null) payload.ok = Number(data.ok) ? 1 : 0;
      // 同钓结算 / 表情
      if (data.gr) payload.gr = String(data.gr).slice(0, 8);
      if (data.ft != null) payload.ft = Math.min(3600, Math.max(0, Number(data.ft) || 0));
      if (data.pm != null) payload.pm = Math.min(3, Math.max(0, Number(data.pm) || 0));
      if (data.cb != null) payload.cb = Math.min(999, Math.max(0, Number(data.cb) || 0));
      if (data.host != null) payload.host = Number(data.host) ? 1 : 0;
      if (data.xp != null) payload.xp = Math.min(999999, Math.max(0, Number(data.xp) || 0));
      if (data.emo) payload.emo = String(data.emo).slice(0, 16);
      if (data.etxt) payload.etxt = String(data.etxt).slice(0, 16);
      const targets = [];
      if (client.roomId) {
        const room = rooms.get(client.roomId);
        if (room) {
          for (const mid of room.members) {
            if (mid === id) continue;
            const c = clients.get(mid);
            if (!c) continue;
            if (String(c.spot || "") !== client.spot) continue;
            targets.push(c);
          }
        }
      } else {
        for (const c of clients.values()) {
          if (c.id === id || c.roomId) continue;
          if (String(c.spot || "") !== client.spot) continue;
          targets.push(c);
        }
      }
      for (const c of targets) send(c.ws, payload);
      return;
    }
    if (t === "pvp") {
      if (!client.roomId) {
        send(ws, { t: "error", text: "不在房间内，无法切磋" });
        return;
      }
      const room = rooms.get(client.roomId);
      if (!room) {
        send(ws, { t: "error", text: "房间已解散" });
        return;
      }
      const payload = Object.assign({}, data, {
        t: "pvp",
        from: id,
        from_name: String(data.from_name || client.name || "钓客").slice(0, 12),
      });
      for (const mid of room.members) {
        const c = clients.get(mid);
        if (c) send(c.ws, payload);
      }
      return;
    }
  });

  ws.on("close", () => {
    const spot = client.spot;
    const wasRoom = !!client.roomId;
    leaveRoom(client);
    clients.delete(id);
    if (!wasRoom) emitSpotPresence(spot);
  });
});

server.listen(PORT, () => {
  console.log(`ChuXinDiao net listening on :${PORT}  path /ws`);
});
