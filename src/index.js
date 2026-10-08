/* Road to Glory - realtime relay on Cloudflare (Worker + one Durable Object).
   Keeps the latest state of every online player and broadcasts changes 20x/second.
   Story, money, police and NPCs stay local in each browser. */
import { DurableObject } from "cloudflare:workers";

export default {
  async fetch(request, env) {
    if (request.headers.get("Upgrade") === "websocket") {
      return env.ROOM.get(env.ROOM.idFromName("main")).fetch(request);
    }
    return new Response("RTG server OK", { headers: { "Access-Control-Allow-Origin": "*" } });
  },
};

const TICK = 50, MAXP = 24, STALE = 25000;
const NUM = new Set(["o", "x", "y", "a", "m", "d", "i", "rs", "t"]);
const STR = { n: 40, av: 400, v: 24, c: 12, lk: 700, rd: 80 };

function clean(d) {
  const o = {};
  if (!d || typeof d !== "object" || Array.isArray(d)) return o;
  for (const k in d) {
    const v = d[k];
    if (NUM.has(k)) {
      if (v === null) o[k] = null;
      else if (typeof v === "number" && isFinite(v)) o[k] = Math.max(-1e7, Math.min(1e7, v));
    } else if (STR[k]) {
      if (v === null) o[k] = null;
      else if (typeof v === "string" && v.length <= STR[k]) o[k] = v;
    }
  }
  return o;
}
function merge(t, d) { for (const k in d) { if (d[k] === null) delete t[k]; else t[k] = d[k]; } }

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.players = new Map(); // uid -> { ws, state, pend, seen }
    this.timer = null; this.n = 0;
    this.origins = String(env.ORIGINS || "").split(",").map(s => s.trim()).filter(Boolean);
  }

  async fetch(request) {
    if (this.origins.length && this.origins.indexOf(request.headers.get("Origin")) < 0) {
      return new Response("Forbidden", { status: 403 });
    }
    const pair = new WebSocketPair();
    const client = pair[0], ws = pair[1];
    ws.accept();
    ws.uid = null; ws.cnt = 0;
    ws.addEventListener("message", (e) => this.onMessage(ws, e.data));
    const gone = () => this.onClose(ws);
    ws.addEventListener("close", gone);
    ws.addEventListener("error", gone);
    this.startTimer();
    return new Response(null, { status: 101, webSocket: client });
  }

  onMessage(ws, raw) {
    if (typeof raw !== "string" || raw.length > 2048 || ++ws.cnt > 60) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== "object") return;

    if (m.t === "hi") {
      const u = m.u;
      if (ws.uid || typeof u !== "string" || !u || u.length > 60) return;
      if (!this.players.has(u) && this.players.size >= MAXP) { try { ws.close(4001, "full"); } catch (e) {} return; }
      const old = this.players.get(u);
      if (old && old.ws !== ws) { old.ws.uid = null; try { old.ws.close(4000, "replaced"); } catch (e) {} }
      ws.uid = u; this.startTimer();
      this.players.set(u, { ws, state: {}, pend: {}, seen: Date.now() });
      const l = [];
      this.players.forEach((p, id) => { if (id !== u && p.state.n) l.push([id, p.state]); });
      ws.send(JSON.stringify({ t: "full", l }));
      return;
    }
    if (m.t === "u" && ws.uid) {
      const p = this.players.get(ws.uid); if (!p) return;
      const d = clean(m.d);
      if (m.r) { p.state = {}; p.pend = Object.assign({}, d); }
      merge(p.state, d);
      if (!m.r) merge(p.pend, d);
      p.seen = Date.now();
    }
  }

  onClose(ws) {
    if (!ws.uid) return;
    const p = this.players.get(ws.uid);
    if (p && p.ws === ws) {
      this.players.delete(ws.uid);
      const s = JSON.stringify({ t: "x", u: ws.uid });
      this.players.forEach((q) => { try { q.ws.send(s); } catch (e) {} });
    }
    ws.uid = null;
  }

  startTimer() {
    if (this.timer) return;
    this.timer = setInterval(() => this.tick(), TICK);
  }

  tick() {
    this.n++;
    const changed = [];
    this.players.forEach((p, id) => { if (Object.keys(p.pend).length) { changed.push([id, p.pend]); p.pend = {}; } });
    if (changed.length) {
      this.players.forEach((p, id) => {
        const l = changed.filter(c => c[0] !== id);
        if (l.length) { try { p.ws.send(JSON.stringify({ t: "b", l })); } catch (e) {} }
      });
    }
    if (this.n % 20 === 0) this.players.forEach((p) => { p.ws.cnt = 0; });
    if (this.n % 200 === 0) {
      const now = Date.now();
      this.players.forEach((p) => { if (now - p.seen > STALE) { try { p.ws.close(4002, "idle"); } catch (e) {} } });
    }
    // nobody online: stop the timer so the object can go idle (free plan friendly)
    if (!this.players.size && this.n % 100 === 0) { clearInterval(this.timer); this.timer = null; }
  }
}
