package raft

// Live, interactive view of a Raft cluster, for learning and debugging.
// Not part of the graded tests: it only runs when RAFT_VIZ is set.
//
//   RAFT_VIZ=1 go test -count=1 -run TestViz -timeout 0 ./raft
//   then open http://localhost:8080
//
// Optional: RAFT_VIZ_N=3 (number of servers, default 5)
//           RAFT_VIZ_PORT=8081 (default 8080)
//
// It only reads Raft state through vizSnapshot (which takes rf.mu and
// copies), and only changes things through the tester's own functions
// (disconnect, connect, crash1, start1) and Start.

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

type vizServer struct {
	ID          int           `json:"id"`
	Alive       bool          `json:"alive"`
	Connected   bool          `json:"connected"`
	State       string        `json:"state"`
	Term        int           `json:"term"`
	VotedFor    int           `json:"votedFor"`
	LogTerms    []int         `json:"logTerms"`
	LogCmds     []interface{} `json:"logCmds"`
	CommitIndex int           `json:"commitIndex"`
	LastApplied int           `json:"lastApplied"`
	NextIndex   []int         `json:"nextIndex,omitempty"`
	MatchIndex  []int         `json:"matchIndex,omitempty"`
	Applied     int           `json:"applied"` // entries the tester received on applyCh
}

type vizEvent struct {
	Time string `json:"time"`
	Msg  string `json:"msg"`
}

type viz struct {
	mu      sync.Mutex // one action or snapshot at a time
	cfg     *config
	nextCmd int
	events  []vizEvent
	quit    chan struct{}
	quitted bool
}

// copies one server's state while holding its lock.
func (rf *Raft) vizSnapshot() vizServer {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	s := vizServer{
		ID:          rf.me,
		Alive:       !rf.killed(),
		Term:        rf.currentTerm,
		VotedFor:    rf.votedFor,
		CommitIndex: rf.commitIndex,
		LastApplied: rf.lastApplied,
	}
	switch rf.state {
	case Leader:
		s.State = "leader"
	case Candidate:
		s.State = "candidate"
	default:
		s.State = "follower"
	}
	for _, e := range rf.log {
		s.LogTerms = append(s.LogTerms, e.Term)
		s.LogCmds = append(s.LogCmds, e.Command)
	}
	if rf.state == Leader {
		s.NextIndex = append([]int{}, rf.nextIndex...)
		s.MatchIndex = append([]int{}, rf.matchIndex...)
	}
	return s
}

func (v *viz) logEventLocked(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	v.events = append(v.events, vizEvent{Time: time.Now().Format("15:04:05.000"), Msg: msg})
	if len(v.events) > 200 {
		v.events = v.events[len(v.events)-200:]
	}
	fmt.Println("[viz]", msg)
}

func (v *viz) handleState(w http.ResponseWriter, r *http.Request) {
	v.mu.Lock()
	defer v.mu.Unlock()
	cfg := v.cfg

	servers := make([]vizServer, cfg.n)
	for i := 0; i < cfg.n; i++ {
		cfg.mu.Lock()
		rf := cfg.rafts[i]
		applied := len(cfg.logs[i])
		cfg.mu.Unlock()

		if rf != nil {
			servers[i] = rf.vizSnapshot()
		} else {
			servers[i] = vizServer{ID: i, State: "crashed", VotedFor: -1}
		}
		servers[i].Connected = cfg.connected[i]
		servers[i].Applied = applied
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"servers": servers,
		"events":  v.events,
	})
}

func (v *viz) handleAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	cfg := v.cfg

	op := r.URL.Query().Get("op")
	i := -1
	if s := r.URL.Query().Get("server"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 || n >= cfg.n {
			http.Error(w, "bad server", http.StatusBadRequest)
			return
		}
		i = n
	}
	needServer := func() bool {
		if i < 0 {
			http.Error(w, "missing server", http.StatusBadRequest)
			return false
		}
		return true
	}
	raftAt := func(i int) *Raft {
		cfg.mu.Lock()
		defer cfg.mu.Unlock()
		return cfg.rafts[i]
	}

	switch op {
	case "disconnect":
		if !needServer() {
			return
		}
		cfg.disconnect(i)
		v.logEventLocked("disconnect S%d", i)

	case "connect":
		if !needServer() {
			return
		}
		if raftAt(i) == nil {
			http.Error(w, "server is crashed: restart it instead", http.StatusBadRequest)
			return
		}
		cfg.connect(i)
		v.logEventLocked("reconnect S%d", i)

	case "crash":
		if !needServer() {
			return
		}
		cfg.crash1(i)
		v.logEventLocked("crash S%d (Kill)", i)

	case "restart":
		if !needServer() {
			return
		}
		cfg.start1(i) // calls Make() again: a brand-new, empty server
		cfg.mu.Lock()
		cfg.logs[i] = map[int]int{} // forget what the old instance applied
		cfg.mu.Unlock()
		cfg.connect(i)
		v.logEventLocked("restart S%d (fresh Make, empty log)", i)

	case "start":
		cmd := v.nextCmd
		if s := r.URL.Query().Get("cmd"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil {
				http.Error(w, "cmd must be an int (the tester only accepts ints)", http.StatusBadRequest)
				return
			}
			cmd = n
		}
		targets := []int{}
		if i >= 0 {
			targets = append(targets, i)
		} else {
			for j := 0; j < cfg.n; j++ {
				if cfg.connected[j] {
					targets = append(targets, j)
				}
			}
		}
		accepted := false
		for _, j := range targets {
			rf := raftAt(j)
			if rf == nil {
				continue
			}
			index, term, ok := rf.Start(cmd)
			if ok {
				v.logEventLocked("Start(%d) on S%d -> index %d, term %d", cmd, j, index, term)
				accepted = true
				break
			}
		}
		if !accepted {
			v.logEventLocked("Start(%d): no server accepted (no leader?)", cmd)
		} else if r.URL.Query().Get("cmd") == "" {
			v.nextCmd++
		}

	case "quit":
		if !v.quitted {
			v.quitted = true
			v.logEventLocked("quit")
			close(v.quit)
		}

	default:
		http.Error(w, "unknown op", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func vizEnvInt(name string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil && n > 0 {
		return n
	}
	return def
}

func TestViz(t *testing.T) {
	if os.Getenv("RAFT_VIZ") == "" {
		t.Skip("set RAFT_VIZ=1 to run the live visualizer")
	}
	n := vizEnvInt("RAFT_VIZ_N", 5)
	port := vizEnvInt("RAFT_VIZ_PORT", 8080)

	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("can't listen on port %d (try RAFT_VIZ_PORT=8081): %v", port, err)
	}

	cfg := make_config(t, n, false)
	defer cfg.cleanup()

	v := &viz{cfg: cfg, nextCmd: 100, quit: make(chan struct{})}
	v.mu.Lock()
	v.logEventLocked("started %d servers", n)
	v.mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, vizPage)
	})
	mux.HandleFunc("/state", v.handleState)
	mux.HandleFunc("/action", v.handleAction)
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)

	fmt.Printf("\nRaft visualizer running: http://localhost:%d\n", port)
	fmt.Printf("Press Quit on the page (or Ctrl+C here) to stop.\n\n")

	<-v.quit
	srv.Close()
}

// The page. Plain JS, no libraries. (No backticks inside: this is a Go raw string.)
const vizPage = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Raft Live View</title>
<style>
:root {
  --bg: #f6f6f4; --panel: #ffffff; --text: #1d1d1b; --muted: #6b6b66; --line: #e2e1dc;
  --leader: #2f7d4f; --candidate: #b7791f; --follower: #5b6b7c; --crashed: #9b2c2c;
  --accent: #2b5fd9;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #161615; --panel: #20201e; --text: #ecebe6; --muted: #9c9b95; --line: #34332f;
    --leader: #4fb37a; --candidate: #e0a84a; --follower: #8a9bb0; --crashed: #e07070;
    --accent: #7aa2ff;
  }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text);
  font: 14px/1.4 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
header { display: flex; flex-wrap: wrap; gap: 10px; align-items: center;
  padding: 12px 16px; border-bottom: 1px solid var(--line); background: var(--panel);
  position: sticky; top: 0; z-index: 2; }
header h1 { font-size: 16px; margin: 0 12px 0 0; }
button { font: inherit; padding: 5px 10px; border-radius: 6px; cursor: pointer;
  border: 1px solid var(--line); background: var(--bg); color: var(--text); }
button:hover { border-color: var(--accent); }
button.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
button.danger:hover { border-color: var(--crashed); color: var(--crashed); }
input[type=number] { width: 80px; font: inherit; padding: 4px 6px; border-radius: 6px;
  border: 1px solid var(--line); background: var(--bg); color: var(--text); }
label { display: inline-flex; gap: 6px; align-items: center; color: var(--muted); }
#status { margin-left: auto; color: var(--muted); font-size: 12px; }
main { display: grid; grid-template-columns: minmax(0, 1fr) 340px; gap: 16px; padding: 16px; }
@media (max-width: 900px) { main { grid-template-columns: 1fr; } }
#servers { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 12px; align-content: start; }
.card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px;
  border-top: 4px solid var(--follower); }
.card.leader { border-top-color: var(--leader); }
.card.candidate { border-top-color: var(--candidate); }
.card.crashed { border-top-color: var(--crashed); opacity: .75; }
.card.off { background-image: repeating-linear-gradient(45deg, transparent 0 10px, rgba(128,128,128,.07) 10px 20px); }
.top { display: flex; align-items: center; gap: 8px; }
.name { font-weight: 700; font-size: 18px; }
.pill { font-size: 12px; padding: 1px 8px; border-radius: 999px; color: #fff; background: var(--follower); text-transform: uppercase; letter-spacing: .03em; }
.pill.leader { background: var(--leader); } .pill.candidate { background: var(--candidate); } .pill.crashed { background: var(--crashed); }
.badge { font-size: 12px; color: var(--crashed); border: 1px solid var(--crashed); border-radius: 999px; padding: 0 7px; }
.term { margin-left: auto; color: var(--muted); }
.term b { color: var(--text); font-size: 16px; }
.stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; margin: 10px 0; }
.stat { background: var(--bg); border-radius: 6px; padding: 4px 6px; }
.stat span { display: block; font-size: 11px; color: var(--muted); }
.stat b { font-variant-numeric: tabular-nums; }
.log { display: flex; flex-wrap: wrap; gap: 3px; min-height: 30px; padding: 6px; background: var(--bg); border-radius: 6px; }
.e { width: 26px; height: 26px; border-radius: 4px; display: flex; align-items: center; justify-content: center;
  font-size: 11px; font-weight: 600; color: #fff; position: relative; }
.e.uncommitted { opacity: .45; outline: 2px dashed currentColor; outline-offset: -2px; }
.e.unapplied::after { content: ""; position: absolute; bottom: 2px; left: 50%; width: 4px; height: 4px; margin-left: -2px; border-radius: 50%; background: #fff; }
.empty { color: var(--muted); font-size: 12px; align-self: center; }
.legend { font-size: 11px; color: var(--muted); margin-top: 4px; }
table { width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 12px; font-variant-numeric: tabular-nums; }
th, td { text-align: center; padding: 2px 4px; border-bottom: 1px solid var(--line); }
th:first-child, td:first-child { text-align: left; color: var(--muted); }
.actions { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 10px; }
.actions button { font-size: 12px; padding: 3px 8px; }
aside { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px; align-self: start;
  position: sticky; top: 70px; max-height: calc(100vh - 90px); display: flex; flex-direction: column; }
aside h2 { font-size: 14px; margin: 0 0 8px; }
#events { overflow-y: auto; font-size: 12px; font-family: ui-monospace, Menlo, monospace; }
#events div { padding: 2px 0; border-bottom: 1px solid var(--line); }
#events .t { color: var(--muted); margin-right: 6px; }
#events .you { color: var(--accent); }
.help { font-size: 12px; color: var(--muted); margin-top: 10px; border-top: 1px solid var(--line); padding-top: 8px; }
</style>
</head>
<body>
<header>
  <h1>Raft Live View</h1>
  <button class="primary" onclick="send()">Send command</button>
  <label>value <input id="cmd" type="number" placeholder="auto"></label>
  <label><input id="auto" type="checkbox"> auto-send every 1 s</label>
  <button class="danger" onclick="act('quit')">Quit</button>
  <span id="status">connecting...</span>
</header>
<main>
  <section id="servers"></section>
  <aside>
    <h2>What happened</h2>
    <div id="events"></div>
    <div class="help">
      <b>Blue</b> lines are your actions. The rest are changes noticed between refreshes.<br>
      Log squares: number = term. Dashed = not committed yet. White dot = committed but not applied yet.
    </div>
  </aside>
</main>
<script>
var prev = null;
var seenEvents = 0;
var feed = [];
var lastHTML = "";

function btn(op, id, label, cls) {
  return '<button class="' + cls + '" data-op="' + op + '" data-server="' + id + '">' + label + '</button>';
}
document.addEventListener("click", function (ev) {
  var b = ev.target.closest("button[data-op]");
  if (!b) return;
  var id = Number(b.getAttribute("data-server"));
  var op = b.getAttribute("data-op");
  if (op === "start") send(id); else act(op, id);
});

function termColor(t) {
  if (t === 0) return "#888";
  return "hsl(" + ((t * 67) % 360) + ", 55%, 45%)";
}
function esc(s) {
  return String(s).replace(/[&<>"]/g, function (c) {
    return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
  });
}
function addFeed(time, msg, you) {
  feed.unshift({ t: time, m: msg, you: you });
  if (feed.length > 300) feed.length = 300;
}
function nowStr() {
  var d = new Date();
  return d.toTimeString().slice(0, 8) + "." + String(d.getMilliseconds()).padStart(3, "0");
}

function act(op, server, extra) {
  var q = "op=" + op;
  if (server !== undefined && server !== null) q += "&server=" + server;
  if (extra) q += "&" + extra;
  return fetch("/action?" + q, { method: "POST" }).then(function (r) {
    if (!r.ok) return r.text().then(function (t) { addFeed(nowStr(), op + ": " + t.trim(), true); });
  }).catch(function () {});
}
function send(server) {
  var v = document.getElementById("cmd").value;
  act("start", server, v ? "cmd=" + encodeURIComponent(v) : "");
}

function diff(a, b) {
  var t = nowStr();
  for (var i = 0; i < b.length; i++) {
    var o = a[i], n = b[i];
    if (!o) continue;
    if (o.state !== n.state) addFeed(t, "S" + i + ": " + o.state + " -> " + n.state + (n.state !== "crashed" ? " (term " + n.term + ")" : ""), false);
    else if (o.term !== n.term) addFeed(t, "S" + i + ": term " + o.term + " -> " + n.term, false);
    if (o.votedFor !== n.votedFor && n.votedFor >= 0 && n.state !== "leader") addFeed(t, "S" + i + " voted for S" + n.votedFor + " (term " + n.term + ")", false);
    if (n.commitIndex > o.commitIndex) addFeed(t, "S" + i + ": commit " + o.commitIndex + " -> " + n.commitIndex, false);
    var ol = o.logTerms ? o.logTerms.length : 0, nl = n.logTerms ? n.logTerms.length : 0;
    if (n.state !== "crashed" && o.state !== "crashed" && nl < ol) addFeed(t, "S" + i + ": log cut from " + (ol - 1) + " to " + (nl - 1) + " entries (conflict repair)", false);
  }
}

function render(data) {
  var servers = data.servers;
  if (prev) diff(prev, servers);
  prev = servers;

  var evs = data.events || [];
  if (evs.length < seenEvents) seenEvents = 0;
  for (var k = seenEvents; k < evs.length; k++) addFeed(evs[k].time, evs[k].msg, true);
  seenEvents = evs.length;

  var html = "";
  servers.forEach(function (s) {
    var crashed = s.state === "crashed";
    var cls = "card " + s.state + (s.connected ? "" : " off");
    html += '<div class="' + cls + '">';
    html += '<div class="top"><span class="name">S' + s.id + '</span><span class="pill ' + s.state + '">' + s.state + '</span>';
    if (!crashed && !s.connected) html += '<span class="badge">disconnected</span>';
    if (!crashed) html += '<span class="term">term <b>' + s.term + '</b></span>';
    html += '</div>';

    if (!crashed) {
      var last = s.logTerms.length - 1;
      html += '<div class="stats">' +
        '<div class="stat"><span>last index</span><b>' + last + '</b></div>' +
        '<div class="stat"><span>commitIndex</span><b>' + s.commitIndex + '</b></div>' +
        '<div class="stat"><span>lastApplied</span><b>' + s.lastApplied + '</b></div>' +
        '<div class="stat"><span>voted for</span><b>' + (s.votedFor < 0 ? "-" : "S" + s.votedFor) + '</b></div>' +
        '</div>';

      html += '<div class="log">';
      if (last === 0) html += '<span class="empty">empty log (just the dummy at index 0)</span>';
      for (var i = 1; i <= last; i++) {
        var c = "e";
        if (i > s.commitIndex) c += " uncommitted";
        else if (i > s.lastApplied) c += " unapplied";
        html += '<div class="' + c + '" style="background:' + termColor(s.logTerms[i]) + ';color:' + termColor(s.logTerms[i]) + '" title="index ' + i +
          ", term " + s.logTerms[i] + ", cmd " + esc(s.logCmds[i]) + '"><span style="color:#fff">' + s.logTerms[i] + '</span></div>';
      }
      html += '</div>';
      html += '<div class="legend">tester received ' + s.applied + ' entr' + (s.applied === 1 ? 'y' : 'ies') + ' on applyCh</div>';

      if (s.state === "leader" && s.nextIndex) {
        html += '<table><tr><th></th>';
        for (var p = 0; p < s.nextIndex.length; p++) html += '<th>S' + p + (p === s.id ? ' (me)' : '') + '</th>';
        html += '</tr><tr><td>nextIndex</td>';
        for (p = 0; p < s.nextIndex.length; p++) html += '<td>' + (p === s.id ? '-' : s.nextIndex[p]) + '</td>';
        html += '</tr><tr><td>matchIndex</td>';
        for (p = 0; p < s.matchIndex.length; p++) html += '<td>' + s.matchIndex[p] + '</td>';
        html += '</tr></table>';
      }
    } else {
      html += '<div class="legend" style="margin:10px 0">Killed. Restart calls Make() again: it comes back as a fresh follower with an empty log (this lab has no saved state).</div>';
    }

    html += '<div class="actions">';
    if (crashed) {
      html += btn("restart", s.id, "Restart", "");
    } else {
      if (s.connected) html += btn("disconnect", s.id, "Disconnect", "");
      else html += btn("connect", s.id, "Reconnect", "");
      html += btn("crash", s.id, "Kill", "danger");
      html += btn("start", s.id, "Start() here", "");
    }
    html += '</div></div>';
  });
  // only redraw when something changed, so buttons aren't replaced mid-click
  if (html !== lastHTML) {
    document.getElementById("servers").innerHTML = html;
    lastHTML = html;
  }

  document.getElementById("events").innerHTML = feed.map(function (f) {
    return '<div class="' + (f.you ? "you" : "") + '"><span class="t">' + esc(f.t) + '</span>' + esc(f.m) + '</div>';
  }).join("");
}

function poll() {
  fetch("/state").then(function (r) { return r.json(); }).then(function (d) {
    document.getElementById("status").textContent = "live - updated " + nowStr();
    render(d);
  }).catch(function () {
    document.getElementById("status").textContent = "not connected (is the test still running?)";
  }).then(function () { setTimeout(poll, 250); });
}
setInterval(function () { if (document.getElementById("auto").checked) send(); }, 1000);
poll();
</script>
</body>
</html>
`
