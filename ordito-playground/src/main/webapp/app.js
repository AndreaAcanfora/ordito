"use strict";
// The page is a thin view over the Java simulator (ordito.js, compiled by TeaVM). Every action is
// recorded with the simulated time it happened at; going back in time re-creates the simulation from
// its seed and replays the actions, which works because the simulation is deterministic.

const $ = (id) => document.getElementById(id);
const NS = "http://www.w3.org/2000/svg";
const R = 200; // ring radius in svg units

const BUGS = [
  { bug: "stale-reads", nodes: 5, seed: 8, title: "Stale reads",
    text: "The leader answers reads from its own state at once: it may no longer lead, or not yet know what is committed." },
  { bug: "vote-for-stale-log", nodes: 5, seed: 1, title: "Votes for a stale log",
    text: "Nodes vote without checking the candidate has every committed entry." },
  { bug: "forget-vote", nodes: 3, seed: 146, title: "Forgotten vote",
    text: "The vote is not saved to disk, so a restarted node votes twice in one term." },
  { bug: "commit-previous-term", nodes: 3, seed: 1071, title: "Figure 8",
    text: "Old-term entries are committed by counting copies, the trap in figure 8 of the Raft paper." },
  { bug: "lying-fsync", nodes: 3, seed: 1, title: "A disk that lies",
    text: "fsync returns before the data is durable, so a crash loses what was acknowledged." },
];

const sim = {
  config: { seed: 1, nodes: 5, bug: "", replay: false },
  actions: [],      // {t, fn, args}
  next: 0,          // first action not yet applied
  horizon: 0,       // furthest time reached
  playing: true,
  speed: 100,       // simulated ms per real second
  state: null,
  scenario: null,
  stopAt: null,     // pause here (just before a known failure)
};

// ---- talking to the simulator ----

function create() {
  const c = sim.config;
  window.orditoCreate(c.seed, c.nodes, c.bug, c.replay);
  sim.next = 0;
}

const ACTIONS = {
  crash: (id) => window.orditoCrash(id),
  restart: (id) => window.orditoRestart(id),
  pause: (id, ms) => window.orditoPause(id, ms),
  resume: (id) => window.orditoResume(id),
  isolate: (id) => window.orditoIsolate(id),
  link: (a, b, cut) => window.orditoSetLink(a, b, cut),
  heal: () => window.orditoHeal(),
  faults: (on) => window.orditoSetFaults(on),
  clients: (on) => window.orditoSetClients(on),
  write: (k, v) => window.orditoWrite(k, v),
};

function act(fn, ...args) {
  const t = window.orditoNow();
  sim.actions = sim.actions.slice(0, sim.next).filter((a) => a.t <= t); // a new action rewrites the future
  sim.actions.push({ t, fn, args });
  sim.next = sim.actions.length;
  ACTIONS[fn](...args);
  sim.horizon = t;
  refresh();
}

function runTo(target) {
  while (sim.next < sim.actions.length && sim.actions[sim.next].t <= target) {
    const a = sim.actions[sim.next++];
    window.orditoRunTo(a.t);
    ACTIONS[a.fn](...a.args);
  }
  window.orditoRunTo(target);
}

function goTo(t) {
  create();
  runTo(t);
  hideBanner();
  refresh();
}

function restart(config) {
  sim.config = config;
  sim.actions = [];
  sim.horizon = 0;
  sim.stopAt = null;
  hideBanner();
  $("verdictBox").hidden = true;
  create();
  if (!config.replay) {
    act("clients", $("clients").checked);
    if ($("faults").checked) act("faults", true);
  }
  setPlaying(true);
  refresh();
}

// ---- watching a bug ----

function failureOf(run) {
  // Runs a scenario to its end at full speed and reports when and how it fails.
  create();
  let t = 0;
  while (t < 24100) {
    t += 100;
    window.orditoRunTo(t);
    const s = JSON.parse(window.orditoState());
    if (s.failure !== "NONE") return { time: s.now, message: s.failureMessage, kind: s.failure };
  }
  const v = JSON.parse(window.orditoCheck());
  if (v.verdict === "NOT_LINEARIZABLE") {
    return { time: v.culprit[0].returned, message: v.explanation, kind: "LINEARIZABILITY", verdict: v };
  }
  return null;
}

function watch(scenario) {
  sim.scenario = scenario;
  history.replaceState(null, "", `?bug=${scenario.bug}&seed=${scenario.seed}&nodes=${scenario.nodes}`);
  document.querySelectorAll(".bugs button").forEach((b) => b.setAttribute("aria-pressed", String(b.dataset.bug === scenario.bug)));
  restart({ seed: scenario.seed, nodes: scenario.nodes, bug: scenario.bug, replay: true });
  $("seed").value = scenario.seed;
  $("nodes").value = scenario.nodes;
  const failure = failureOf();
  if (!failure) {
    showBanner("This seed no longer fails: the simulator changed since it was recorded.");
    return;
  }
  sim.failure = failure;
  const lead = 1500;
  goTo(Math.max(0, failure.time - lead));
  sim.horizon = failure.time;
  sim.stopAt = failure.time;
  setSpeedValue(sim.requestedSpeed ?? 20);
  setPlaying(true);
  showBanner(`${scenario.title}: the cluster breaks at ${(failure.time / 1000).toFixed(2)} s. Watch.`, true);
}

// ---- rendering ----

// Red means trouble on this page, so terms are dyed with everything but madder.
const DYES = ["--woad", "--weld", "--indigo", "--sage", "--walnut"];
function dye(term) {
  return getComputedStyle(document.documentElement).getPropertyValue(DYES[Math.max(0, term - 1) % DYES.length]).trim();
}

function position(i, n) {
  const a = -Math.PI / 2 + (2 * Math.PI * i) / n;
  return [R * Math.cos(a), R * Math.sin(a)];
}

// Term and commit sit just outside the ring, away from the links.
function metaLabel(x, y, text) {
  const d = Math.hypot(x, y) || 1, ux = x / d, uy = y / d;
  const anchor = ux > 0.3 ? "start" : ux < -0.3 ? "end" : "middle";
  const lx = x + ux * 52, ly = y + uy * 56 + (Math.abs(uy) < 0.5 ? 4 : uy > 0 ? 6 : 0);
  return `<text class="meta" x="${lx}" y="${ly}" style="text-anchor:${anchor}">${text}</text>`;
}

function refresh() {
  sim.state = JSON.parse(window.orditoState());
  sim.horizon = Math.max(sim.horizon, sim.state.now);
}

function draw() {
  const s = sim.state;
  if (!s) return;
  const n = s.nodes.length;
  const pos = {};
  s.nodes.forEach((node, i) => (pos[node.id] = position(i, n)));
  const cut = new Set(s.cut.map(([a, b]) => a + "-" + b));
  let svg = "";
  for (let i = 0; i < n; i++) {
    for (let j = i + 1; j < n; j++) {
      const a = s.nodes[i].id, b = s.nodes[j].id;
      const [x1, y1] = pos[a], [x2, y2] = pos[b];
      const isCut = cut.has(a + "-" + b);
      svg += `<line class="link${isCut ? " cut" : ""}" x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}"/>`;
      svg += `<line class="link-hit" data-a="${a}" data-b="${b}" data-cut="${isCut}" x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}"><title>${isCut ? "Restore" : "Cut"} the link n${a} - n${b}</title></line>`;
    }
  }
  for (const [from, to, sent, arrives, kind] of s.messages) {
    if (!pos[from] || !pos[to]) continue;
    const p = Math.min(1, Math.max(0, (s.now - sent) / Math.max(1, arrives - sent)));
    const [x1, y1] = pos[from], [x2, y2] = pos[to];
    const k = kind.startsWith("append") ? "append" : kind;
    const r = k === "append" ? 6 : k === "heartbeat" ? 4 : 5;
    svg += `<circle class="msg ${k}" r="${r}" cx="${x1 + (x2 - x1) * p}" cy="${y1 + (y2 - y1) * p}"/>`;
  }
  for (const node of s.nodes) {
    const [x, y] = pos[node.id];
    const role = !node.up ? "down" : (node.role || "").toLowerCase();
    const label = !node.up ? "crashed" : node.paused ? "paused" : role === "pre_candidate" ? "pre-vote" : role;
    const ring = node.up && node.role === "LEADER" ? `stroke="${dye(node.term)}"` : "";
    svg += `<g class="node ${role}" data-id="${node.id}" tabindex="0" role="button" aria-label="Node ${node.id}, ${label}">
      <circle class="body" r="40" cx="${x}" cy="${y}" ${ring}/>
      <text class="name" x="${x}" y="${y - 4}">n${node.id}</text>
      <text class="role" x="${x}" y="${y + 12}" fill="${node.up && node.role === "LEADER" ? dye(node.term) : "currentColor"}">${label}</text>
      ${node.up ? metaLabel(x, y, `term ${node.term}, commit ${node.commit}`) : ""}
      ${node.paused ? `<text class="name" x="${x}" y="${y - 50}">❚❚</text>` : ""}
    </g>`;
  }
  $("ring").innerHTML = svg;
  $("clock").textContent = (s.now / 1000).toFixed(2) + " s";
  const scrub = $("scrub");
  scrub.max = String(sim.horizon);
  if (document.activeElement !== scrub) scrub.value = String(s.now);
  $("faults").checked = s.faults;
  $("clients").checked = s.clients;
}

let loomFrame = 0;
function drawLoom() {
  const s = sim.state;
  if (!s || loomFrame++ % 6 !== 0) return;
  const last = Math.max(0, ...s.nodes.map((n) => n.lastIndex || 0));
  const width = Math.max(10, Math.min(48, Math.floor(($("loom").clientWidth - 70) / 20)));
  const from = Math.max(1, last - width + 1);
  let html = "";
  for (const node of s.nodes) {
    html += `<div class="thread"><span class="who${node.up ? "" : " down"}">n${node.id}</span><div class="cells">`;
    const byIndex = new Map((node.log || []).map((e) => [e[0], e]));
    for (let i = from; i <= last; i++) {
      const e = byIndex.get(i);
      if (!e || !node.up) {
        html += `<span class="cell empty"></span>`;
        continue;
      }
      const cls = (i > node.commit ? " pending" : "") + (i === node.commit ? " commit-edge" : "");
      html += `<span class="cell${cls}" style="background-color:${dye(e[1])}" title="#${e[0]}, term ${e[1]}: ${e[2]}${i > node.commit ? " (not committed)" : ""}">${e[1]}</span>`;
    }
    html += `</div></div>`;
  }
  $("loom").innerHTML = html;
  const events = $("events");
  events.innerHTML = s.log.map((line) => {
    const cls = line.includes("VIOLATION") ? "violation" : line.includes("leader of term") ? "leader" : "";
    return `<li class="${cls}">${escape(line)}</li>`;
  }).join("");
  events.scrollTop = events.scrollHeight;
}

function escape(t) {
  return t.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

// ---- the loop ----

let lastFrame = performance.now();
function frame(t) {
  const dt = Math.min(100, t - lastFrame);
  lastFrame = t;
  if (sim.playing && sim.state && sim.state.failure === "NONE") {
    let target = window.orditoNow() + (dt / 1000) * sim.speed;
    if (sim.stopAt !== null && target >= sim.stopAt) {
      target = sim.stopAt;
    }
    runTo(target);
    refresh();
    if (sim.state.failure !== "NONE") {
      setPlaying(false);
      showBanner(`Caught: ${sim.state.failureMessage}`);
    } else if (sim.stopAt !== null && sim.state.now >= sim.stopAt) {
      setPlaying(false);
      sim.stopAt = null;
      if (sim.failure && sim.failure.verdict) {
        showVerdict(sim.failure.verdict);
        showBanner("Caught by the linearizability checker: a client read a value that was already overwritten.");
      }
    }
  }
  draw();
  drawLoom();
  requestAnimationFrame(frame);
}

// ---- controls ----

function setPlaying(on) {
  sim.playing = on;
  $("play").textContent = on ? "Pause" : "Play";
  $("play").setAttribute("aria-label", on ? "Pause" : "Play");
}

function speedFromSlider(v) {
  return Math.round(10 * Math.pow(500, v / 100));
}

function setSpeedValue(v) {
  $("speed").value = String(v);
  sim.speed = speedFromSlider(v);
  $("speedOut").textContent = `${(sim.speed / 1000).toFixed(sim.speed < 100 ? 3 : 2)}× real time`;
}

function showBanner(text, info) {
  const b = $("banner");
  b.textContent = text;
  b.style.background = info ? "var(--ink)" : "";
  b.style.color = info ? "var(--surface)" : "";
  b.hidden = false;
}

function hideBanner() {
  $("banner").hidden = true;
}

function showVerdict(v) {
  const box = $("verdictBox");
  box.hidden = false;
  const el = $("verdict");
  if (v.verdict === "LINEARIZABLE") {
    el.className = "ok";
    el.textContent = `Linearizable: all ${v.operations} client operations fit one order consistent with real time.`;
    return;
  }
  el.className = "";
  const ops = [v.culprit[0], ...v.concurrent].map((o) =>
    `<div>${(o.called / 1000).toFixed(3)}s → ${o.returned < 0 ? "?" : (o.returned / 1000).toFixed(3) + "s"}  client ${o.client}  ${escape(o.text)}</div>`).join("");
  el.innerHTML = `<p>${escape(v.explanation)}</p><div class="ops">${ops}</div>`;
}

function openPopover(id, anchor) {
  const node = sim.state.nodes.find((n) => n.id === id);
  const pop = $("popover");
  const items = [];
  if (node.up) {
    items.push(["Crash", () => act("crash", id)]);
    items.push(node.paused ? ["Resume", () => act("resume", id)] : ["Pause for 2 s", () => act("pause", id, 2000)]);
    items.push(["Isolate from the others", () => act("isolate", id)]);
  } else {
    items.push(["Restart from its disk", () => act("restart", id)]);
  }
  pop.innerHTML = `<h3>n${id}</h3>`;
  for (const [label, fn] of items) {
    const b = document.createElement("button");
    b.textContent = label;
    b.onclick = () => { fn(); closePopover(); };
    pop.appendChild(b);
  }
  const stage = document.querySelector(".stage").getBoundingClientRect();
  const r = anchor.getBoundingClientRect();
  pop.style.left = `${Math.min(r.right - stage.left + 6, stage.width - 170)}px`;
  pop.style.top = `${r.top - stage.top}px`;
  pop.hidden = false;
  pop.querySelector("button").focus();
}

function closePopover() {
  $("popover").hidden = true;
}

function init() {
  if (typeof window.orditoCreate !== "function") {
    showBanner("ordito.js did not load. Build it with: mvn -Pplayground package");
    return;
  }
  const list = $("bugs");
  for (const b of BUGS) {
    const li = document.createElement("li");
    li.innerHTML = `<button data-bug="${b.bug}" aria-pressed="false"><strong>${b.title}</strong><span>${b.text}</span></button>`;
    li.querySelector("button").onclick = () => watch(b);
    list.appendChild(li);
  }
  $("ring").addEventListener("click", (e) => {
    const node = e.target.closest(".node");
    if (node) {
      openPopover(Number(node.dataset.id), node);
      return;
    }
    const link = e.target.closest(".link-hit");
    if (link) act("link", Number(link.dataset.a), Number(link.dataset.b), link.dataset.cut !== "true");
    closePopover();
  });
  $("ring").addEventListener("keydown", (e) => {
    const node = e.target.closest(".node");
    if (node && (e.key === "Enter" || e.key === " ")) {
      e.preventDefault();
      openPopover(Number(node.dataset.id), node);
    }
  });
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") closePopover(); });
  $("play").onclick = () => setPlaying(!sim.playing);
  $("speed").oninput = (e) => setSpeedValue(Number(e.target.value));
  $("scrub").oninput = (e) => { setPlaying(false); sim.stopAt = null; goTo(Number(e.target.value)); };
  $("clients").onchange = (e) => act("clients", e.target.checked);
  $("faults").onchange = (e) => act("faults", e.target.checked);
  let x = 0;
  $("write").onclick = () => { x++; act("write", "x", String(x)); $("write").textContent = `Write x = ${x + 1}`; };
  $("heal").onclick = () => act("heal");
  $("check").onclick = () => showVerdict(JSON.parse(window.orditoCheck()));
  $("restart").onclick = () => {
    sim.scenario = null;
    history.replaceState(null, "", location.pathname);
    document.querySelectorAll(".bugs button").forEach((b) => b.setAttribute("aria-pressed", "false"));
    restart({ seed: Number($("seed").value) || 1, nodes: Number($("nodes").value), bug: "", replay: false });
  };
  setSpeedValue(35);
  // A link can point at one failure: ?bug=forget-vote&seed=146&nodes=3
  const params = new URLSearchParams(location.search);
  if (params.get("speed")) {
    sim.requestedSpeed = Number(params.get("speed"));
    setSpeedValue(sim.requestedSpeed);
  }
  const known = BUGS.find((b) => b.bug === params.get("bug"));
  if (known) {
    watch({ ...known, seed: Number(params.get("seed")) || known.seed, nodes: Number(params.get("nodes")) || known.nodes });
  } else {
    if (params.get("seed")) $("seed").value = params.get("seed");
    if (params.get("nodes")) $("nodes").value = params.get("nodes");
    sim.config = { seed: Number($("seed").value) || 1, nodes: Number($("nodes").value), bug: "", replay: false };
    restart(sim.config);
  }
  requestAnimationFrame(frame);
}

init();
