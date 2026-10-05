// Road to Campus by Alutta: online features (accounts, leaderboard, wallet, shop, transfers, loans).
// Runs as a Vercel serverless function. Needs an Upstash Redis database connected to the project
// (Vercel dashboard > Storage > Upstash Redis). It reads KV_REST_API_URL / KV_REST_API_TOKEN
// or UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN, whichever the integration provides.
// Points are virtual game points with no cash value. 1 point is shown as 1,000 naira for fun only.

const crypto = require('crypto');

const DB_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const DB_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

// Prices in points. Edit here to change the shop. Each item can be bought once per player.
const CATALOG = [
  { id: 'notebook', e: '📓', name: 'Notebook and pens', price: 5 },
  { id: 'bag', e: '🎒', name: 'School bag', price: 25 },
  { id: 'transcript', e: '📜', name: 'Transcript and certification', price: 30 },
  { id: 'appfee', e: '📝', name: 'Application fee', price: 40 },
  { id: 'coat', e: '🧥', name: 'Winter coat', price: 60 },
  { id: 'medical', e: '🩺', name: 'Medical test', price: 80 },
  { id: 'passport', e: '🛂', name: 'Passport renewal', price: 100 },
  { id: 'english', e: '🗣️', name: 'English test', price: 300 },
  { id: 'laptop', e: '💻', name: 'Laptop', price: 600 },
  { id: 'rent', e: '🏠', name: "First month's rent", price: 800 },
  { id: 'visa', e: '📑', name: 'Visa fee', price: 1000 },
  { id: 'flight', e: '✈️', name: 'Flight ticket', price: 1800 },
  { id: 'deposit', e: '🏫', name: 'Tuition deposit', price: 3000 },
];

// Anti-cheat limits for a single journey.
const SCORE_CAP = { 10: 1500, 16: 2500, 22: 3500 };
const MIN_SECONDS = { 10: 45, 16: 70, 22: 100 };
const MAX_TRANSFER = 100000;
const MAX_LOAN = 20000;
const MAX_PENDING_REQUESTS = 5;
const ROOM_DAYS = 30;
const ROOM_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const NAME_RE = /^[A-Za-z0-9_.]{3,16}$/;
const RESERVED = ['alutta', 'admin', 'moderator', 'support', 'official', 'system'];
const BLOCKED = ['fuck', 'shit', 'bitch', 'cunt', 'dick', 'pussy', 'porn', 'sex', 'nigg', 'fag', 'rape', 'whore', 'slut', 'bastard', 'ashawo', 'mumu'];

/* ---------- Redis over REST ---------- */
async function r(...cmd) {
  const res = await fetch(DB_URL, {
    method: 'POST',
    headers: { Authorization: `Bearer ${DB_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmd),
  });
  const j = await res.json();
  if (j.error) throw new Error(j.error);
  return j.result;
}
async function pipe(cmds) {
  if (!cmds.length) return [];
  const res = await fetch(`${DB_URL}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${DB_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds),
  });
  const j = await res.json();
  return j.map((x) => { if (x.error) throw new Error(x.error); return x.result; });
}
const toObj = (arr) => { const o = {}; if (Array.isArray(arr)) for (let i = 0; i < arr.length; i += 2) o[arr[i]] = arr[i + 1]; return o; };

/* ---------- Atomic scripts ---------- */
const LUA_TRANSFER = `
local b = tonumber(redis.call('GET', KEYS[1]) or '0'); local a = tonumber(ARGV[1])
if b < a then return -1 end
redis.call('DECRBY', KEYS[1], a); redis.call('INCRBY', KEYS[2], a); return b - a`;
const LUA_BUY = `
if redis.call('HEXISTS', KEYS[2], ARGV[2]) == 1 then return -2 end
local b = tonumber(redis.call('GET', KEYS[1]) or '0'); local p = tonumber(ARGV[1])
if b < p then return -1 end
redis.call('DECRBY', KEYS[1], p); redis.call('HSET', KEYS[2], ARGV[2], 1); redis.call('ZINCRBY', KEYS[3], p, ARGV[3]); return b - p`;
const LUA_ACCEPT = `
if redis.call('HGET', KEYS[3], 'status') ~= 'pending' then return -2 end
local a = tonumber(redis.call('HGET', KEYS[3], 'amount'))
local b = tonumber(redis.call('GET', KEYS[1]) or '0')
if b < a then return -1 end
redis.call('DECRBY', KEYS[1], a); redis.call('INCRBY', KEYS[2], a)
redis.call('HSET', KEYS[3], 'status', 'active', 'out', a); return a`;
const LUA_REPAY = `
if redis.call('HGET', KEYS[3], 'status') ~= 'active' then return -2 end
local o = tonumber(redis.call('HGET', KEYS[3], 'out')); local p = math.min(tonumber(ARGV[1]), o)
local b = tonumber(redis.call('GET', KEYS[1]) or '0')
if b < p then return -1 end
redis.call('DECRBY', KEYS[1], p); redis.call('INCRBY', KEYS[2], p)
o = o - p; redis.call('HSET', KEYS[3], 'out', o)
if o == 0 then redis.call('HSET', KEYS[3], 'status', 'repaid') end
return o`;

/* ---------- Helpers ---------- */
class HttpError extends Error { constructor(status, msg) { super(msg); this.status = status; } }
const fail = (status, msg) => { throw new HttpError(status, msg); };
const sha = (s) => crypto.createHash('sha256').update(s).digest('hex');
const hashPin = (pin, salt) => crypto.scryptSync(pin, salt, 32).toString('hex');
const intIn = (v, lo, hi, label) => {
  const n = Number(v);
  if (!Number.isInteger(n) || n < lo || n > hi) fail(400, `${label} must be a whole number from ${lo.toLocaleString()} to ${hi.toLocaleString()}.`);
  return n;
};
async function displayName(u) { return (await r('HGET', `user:${u}`, 'name')) || u; }
async function logEvent(u, text) {
  await pipe([['LPUSH', `log:${u}`, JSON.stringify({ t: Date.now(), text })], ['LTRIM', `log:${u}`, 0, 39]]);
}
async function authUser(req) {
  const h = req.headers.authorization || '';
  const tok = h.startsWith('Bearer ') ? h.slice(7) : '';
  if (!tok) fail(401, 'Please sign in first.');
  const u = await r('GET', `sess:${sha(tok)}`);
  if (!u) fail(401, 'Your session has expired. Please sign in again.');
  return u;
}
async function requireUser(name) {
  const u = String(name || '').trim().toLowerCase();
  if (!NAME_RE.test(u)) fail(400, 'Enter a valid username.');
  const exists = await r('HGET', `user:${u}`, 'name');
  if (!exists) fail(404, `No player called ${name} was found.`);
  return { u, name: exists };
}

/* ---------- Actions ---------- */
async function enter(body) {
  const name = String(body.name || '').trim();
  const pin = String(body.pin || '');
  if (!NAME_RE.test(name)) fail(400, 'Usernames are 3 to 16 letters, numbers, dots or underscores.');
  if (!/^\d{4}$/.test(pin)) fail(400, 'Your PIN must be 4 digits.');
  const u = name.toLowerCase();
  const tries = await r('INCR', `rl:${u}`);
  if (tries === 1) await r('EXPIRE', `rl:${u}`, 900);
  if (tries > 10) fail(429, 'Too many tries for this username. Please wait 15 minutes.');

  let created = false;
  const existing = toObj(await r('HGETALL', `user:${u}`));
  if (!existing.name) {
    if (RESERVED.includes(u) || BLOCKED.some((w) => u.includes(w))) fail(400, 'Please choose a different username.');
    const claimed = await r('HSETNX', `user:${u}`, 'name', name);
    if (!claimed) fail(409, 'That username was just taken. Try another.');
    const salt = crypto.randomBytes(16).toString('hex');
    await pipe([
      ['HSET', `user:${u}`, 'salt', salt, 'pin', hashPin(pin, salt), 'created', Date.now(), 'games', 0],
      ['HSET', 'names', u, name],
      ['SET', `bal:${u}`, 0],
    ]);
    created = true;
  } else {
    const ok = crypto.timingSafeEqual(Buffer.from(hashPin(pin, existing.salt), 'hex'), Buffer.from(existing.pin, 'hex'));
    if (!ok) fail(403, `That username is taken. If it is yours, check your PIN. If not, choose another name.`);
  }
  await r('DEL', `rl:${u}`);
  const token = crypto.randomBytes(24).toString('hex');
  await r('SET', `sess:${sha(token)}`, u, 'EX', 60 * 60 * 24 * 365);
  return { token, name: existing.name || name, created };
}

async function me(u) {
  const [user, bal, items, best, earned, logs, loanIds, rank] = await pipe([
    ['HGETALL', `user:${u}`], ['GET', `bal:${u}`], ['HGETALL', `items:${u}`],
    ['ZSCORE', 'lb:best', u], ['ZSCORE', 'lb:earned', u], ['LRANGE', `log:${u}`, 0, 19],
    ['SMEMBERS', `loans:${u}`], ['ZREVRANK', 'lb:best', u],
  ]);
  const loanRows = await pipe((loanIds || []).map((id) => ['HGETALL', `loan:${id}`]));
  const loans = loanRows.map(toObj).filter((l) => l.id).map((l) => ({
    id: l.id, lender: l.lenderName, borrower: l.borrowerName, amount: Number(l.amount), out: Number(l.out || 0),
    status: l.status, mine: l.borrower === u ? 'borrower' : 'lender', t: Number(l.t),
  })).sort((a, b) => b.t - a.t);
  return {
    name: toObj(user).name, bal: Number(bal || 0), items: Object.keys(toObj(items)),
    best: Number(best || 0), earned: Number(earned || 0), rank: rank === null ? null : rank + 1,
    games: Number(toObj(user).games || 0),
    loans, log: (logs || []).map((x) => { try { return JSON.parse(x); } catch (e) { return null; } }).filter(Boolean),
    pendingIn: loans.filter((l) => l.mine === 'lender' && l.status === 'pending').length,
    catalog: CATALOG,
  };
}

async function getRoom(code) {
  const c = String(code || '').trim().toUpperCase();
  if (!/^[A-Z0-9]{6}$/.test(c)) fail(400, 'Challenge codes are 6 letters and numbers.');
  const room = toObj(await r('HGETALL', `room:${c}`));
  if (!room.code) fail(404, 'No challenge found with that code. Check it and try again.');
  return room;
}
async function roomCreate(u, body) {
  const len = Number(body.len);
  if (!SCORE_CAP[len]) fail(400, 'Unknown journey length.');
  const hostName = await displayName(u);
  for (let tries = 0; tries < 8; tries++) {
    let code = ''; const bytes = crypto.randomBytes(6);
    for (let i = 0; i < 6; i++) code += ROOM_CHARS[bytes[i] % ROOM_CHARS.length];
    const ok = await r('HSETNX', `room:${code}`, 'code', code);
    if (!ok) continue;
    await pipe([
      ['HSET', `room:${code}`, 'len', len, 'host', u, 'hostName', hostName, 't', Date.now()],
      ['EXPIRE', `room:${code}`, 60 * 60 * 24 * ROOM_DAYS],
    ]);
    await logEvent(u, `Created challenge ${code}`);
    return { code, len, hostName };
  }
  fail(500, 'Could not create a challenge. Please try again.');
}
async function roomInfo(body) {
  const room = await getRoom(body.code);
  const players = await r('ZCARD', `room:${room.code}:scores`);
  return { code: room.code, len: Number(room.len), hostName: room.hostName, players };
}
async function roomBoard(body) {
  const room = await getRoom(body.code);
  const flat = await r('ZREVRANGE', `room:${room.code}:scores`, 0, 99, 'WITHSCORES');
  const ids = []; const scores = [];
  for (let i = 0; i < flat.length; i += 2) { ids.push(flat[i]); scores.push(Number(flat[i + 1])); }
  const names = ids.length ? await r('HMGET', 'names', ...ids) : [];
  return { code: room.code, len: Number(room.len), hostName: room.hostName, rows: ids.map((id, i) => ({ id, name: names[i] || id, score: scores[i] })) };
}

async function start(u, body) {
  let len = Number(body.len), room = null;
  if (body.room) { const rm = await getRoom(body.room); room = rm.code; len = Number(rm.len); }
  if (!SCORE_CAP[len]) fail(400, 'Unknown journey length.');
  const ticket = crypto.randomBytes(12).toString('hex');
  await r('SET', `ticket:${ticket}`, JSON.stringify({ u, len, room, t: Date.now() }), 'EX', 60 * 60 * 3);
  return { ticket };
}

async function submit(u, body) {
  const ticket = String(body.ticket || '');
  const raw = ticket && await r('GET', `ticket:${ticket}`);
  if (!raw) fail(400, 'This journey could not be verified, so the points were not saved.');
  const tk = JSON.parse(raw);
  if (tk.u !== u) fail(403, 'This journey belongs to another player.');
  await r('DEL', `ticket:${ticket}`);
  if ((Date.now() - tk.t) / 1000 < MIN_SECONDS[tk.len]) fail(400, 'That journey finished too quickly to count.');
  const score = intIn(body.score, 0, SCORE_CAP[tk.len], 'Score');
  const res = await pipe([
    ['INCRBY', `bal:${u}`, score], ['ZINCRBY', 'lb:earned', score, u], ['ZADD', 'lb:best', 'GT', score, u],
    ['HINCRBY', `user:${u}`, 'games', 1], ['ZREVRANK', 'lb:best', u],
  ]);
  if (score > 0) await logEvent(u, `Earned ${score} pts on a journey`);
  const out = { added: score, bal: Number(res[0]), rank: res[4] === null ? null : res[4] + 1 };
  if (tk.room) {
    const key = `room:${tk.room}:scores`;
    const [added, rk, total] = await pipe([['ZADD', key, 'NX', score, u], ['ZREVRANK', key, u], ['ZCARD', key], ['EXPIRE', key, 60 * 60 * 24 * ROOM_DAYS]]);
    out.room = { code: tk.room, counted: added === 1, rank: rk === null ? null : rk + 1, total };
  }
  return out;
}

async function board(body, req) {
  const key = { best: 'lb:best', earned: 'lb:earned', prep: 'lb:prep' }[body.board] || 'lb:best';
  const flat = await r('ZREVRANGE', key, 0, 49, 'WITHSCORES');
  const ids = []; const scores = [];
  for (let i = 0; i < flat.length; i += 2) { ids.push(flat[i]); scores.push(Number(flat[i + 1])); }
  const names = ids.length ? await r('HMGET', 'names', ...ids) : [];
  const rows = ids.map((id, i) => ({ id, name: names[i] || id, score: scores[i] }));
  let mine = null;
  if (req.headers.authorization) {
    try {
      const u = await authUser(req);
      const [rk, sc] = await pipe([['ZREVRANK', key, u], ['ZSCORE', key, u]]);
      mine = { id: u, rank: rk === null ? null : rk + 1, score: Number(sc || 0) };
    } catch (e) { /* signed out is fine */ }
  }
  const total = await r('ZCARD', key);
  return { rows, mine, total };
}

async function buy(u, body) {
  const item = CATALOG.find((x) => x.id === body.item);
  if (!item) fail(400, 'That item is not in the shop.');
  const left = await r('EVAL', LUA_BUY, 3, `bal:${u}`, `items:${u}`, 'lb:prep', item.price, item.id, u);
  if (left === -2) fail(400, `You already have the ${item.name.toLowerCase()}.`);
  if (left === -1) fail(400, `You need ${item.price} pts for the ${item.name.toLowerCase()}.`);
  await logEvent(u, `Bought ${item.e} ${item.name} for ${item.price} pts`);
  return { bal: Number(left) };
}

async function sendMoney(u, body) {
  const to = await requireUser(body.to);
  if (to.u === u) fail(400, 'You cannot send points to yourself.');
  const amount = intIn(body.amount, 1, MAX_TRANSFER, 'Amount');
  const left = await r('EVAL', LUA_TRANSFER, 2, `bal:${u}`, `bal:${to.u}`, amount);
  if (left === -1) fail(400, 'You do not have enough points for that.');
  const myName = await displayName(u);
  await logEvent(u, `Sent ${amount} pts to ${to.name}`);
  await logEvent(to.u, `Received ${amount} pts from ${myName}`);
  return { bal: Number(left) };
}

async function loanRequest(u, body) {
  const lender = await requireUser(body.from);
  if (lender.u === u) fail(400, 'You cannot borrow from yourself.');
  const amount = intIn(body.amount, 1, MAX_LOAN, 'Amount');
  const ids = await r('SMEMBERS', `loans:${u}`);
  const rows = (await pipe(ids.map((id) => ['HMGET', `loan:${id}`, 'borrower', 'status']))) || [];
  const pending = rows.filter((x) => x && x[0] === u && x[1] === 'pending').length;
  if (pending >= MAX_PENDING_REQUESTS) fail(400, `You already have ${MAX_PENDING_REQUESTS} requests waiting. Cancel one first.`);
  const id = crypto.randomBytes(6).toString('hex');
  const myName = await displayName(u);
  await pipe([
    ['HSET', `loan:${id}`, 'id', id, 'lender', lender.u, 'lenderName', lender.name, 'borrower', u, 'borrowerName', myName,
      'amount', amount, 'out', 0, 'status', 'pending', 't', Date.now()],
    ['SADD', `loans:${u}`, id], ['SADD', `loans:${lender.u}`, id],
  ]);
  await logEvent(u, `Asked ${lender.name} to lend you ${amount} pts`);
  await logEvent(lender.u, `${myName} asked to borrow ${amount} pts`);
  return { id };
}

async function getLoan(id) {
  const l = toObj(await r('HGETALL', `loan:${String(id || '')}`));
  if (!l.id) fail(404, 'That loan was not found.');
  return l;
}
async function closeLoan(l) {
  await pipe([['SREM', `loans:${l.lender}`, l.id], ['SREM', `loans:${l.borrower}`, l.id], ['EXPIRE', `loan:${l.id}`, 60 * 60 * 24 * 30]]);
}

async function loanRespond(u, body) {
  const l = await getLoan(body.id);
  if (l.lender !== u) fail(403, 'Only the lender can answer this request.');
  if (l.status !== 'pending') fail(400, 'This request has already been answered.');
  if (!body.accept) {
    await r('HSET', `loan:${l.id}`, 'status', 'declined');
    await closeLoan(l);
    await logEvent(u, `Declined ${l.borrowerName}'s request for ${l.amount} pts`);
    await logEvent(l.borrower, `${l.lenderName} declined your request for ${l.amount} pts`);
    return { ok: true };
  }
  const res = await r('EVAL', LUA_ACCEPT, 3, `bal:${u}`, `bal:${l.borrower}`, `loan:${l.id}`);
  if (res === -1) fail(400, 'You do not have enough points to lend that amount.');
  if (res === -2) fail(400, 'This request has already been answered.');
  await logEvent(u, `Lent ${l.amount} pts to ${l.borrowerName}`);
  await logEvent(l.borrower, `Borrowed ${l.amount} pts from ${l.lenderName}`);
  return { ok: true };
}

async function loanRepay(u, body) {
  const l = await getLoan(body.id);
  if (l.borrower !== u) fail(403, 'Only the borrower can repay this loan.');
  const amount = intIn(body.amount, 1, MAX_LOAN, 'Amount');
  const left = await r('EVAL', LUA_REPAY, 3, `bal:${u}`, `bal:${l.lender}`, `loan:${l.id}`, amount);
  if (left === -1) fail(400, 'You do not have enough points for that repayment.');
  if (left === -2) fail(400, 'This loan is not active.');
  const paid = Math.min(amount, Number(l.out));
  await logEvent(u, `Repaid ${paid} pts to ${l.lenderName}`);
  await logEvent(l.lender, `${l.borrowerName} repaid ${paid} pts`);
  if (Number(left) === 0) { await closeLoan(l); await logEvent(u, `Loan from ${l.lenderName} fully repaid`); await logEvent(l.lender, `${l.borrowerName} fully repaid your loan`); }
  return { out: Number(left) };
}

async function loanCancel(u, body) {
  const l = await getLoan(body.id);
  if (l.borrower !== u) fail(403, 'Only the borrower can cancel this request.');
  if (l.status !== 'pending') fail(400, 'Only waiting requests can be cancelled.');
  await r('HSET', `loan:${l.id}`, 'status', 'cancelled');
  await closeLoan(l);
  return { ok: true };
}

/* ---------- Handler ---------- */
module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Content-Type', 'application/json');
  if (req.method !== 'POST') { res.statusCode = 405; return res.end(JSON.stringify({ error: 'Use POST.' })); }
  if (!DB_URL || !DB_TOKEN) { res.statusCode = 503; return res.end(JSON.stringify({ error: 'Online storage is not connected yet.' })); }
  let body = req.body;
  if (typeof body === 'string') { try { body = JSON.parse(body); } catch (e) { body = {}; } }
  body = body || {};
  try {
    let out;
    switch (body.action) {
      case 'ping': out = { ok: true }; break;
      case 'enter': out = await enter(body); break;
      case 'board': out = await board(body, req); break;
      case 'catalog': out = { catalog: CATALOG }; break;
      case 'roomInfo': out = await roomInfo(body); break;
      case 'roomBoard': out = await roomBoard(body); break;
      default: {
        const u = await authUser(req);
        const fns = { me, start, submit, roomCreate, buy, send: sendMoney, loanRequest, loanRespond, loanRepay, loanCancel };
        const fn = fns[body.action];
        if (!fn) fail(400, 'Unknown action.');
        out = await fn(u, body);
      }
    }
    res.statusCode = 200; res.end(JSON.stringify(out));
  } catch (e) {
    if (e instanceof HttpError) { res.statusCode = e.status; return res.end(JSON.stringify({ error: e.message })); }
    console.error(e);
    res.statusCode = 500; res.end(JSON.stringify({ error: 'Something went wrong. Please try again.' }));
  }
};
