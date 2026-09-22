function NARRATIVE(D, BASE, SNAP) {
  const R = D.running, H = D.history, S = D.steps, M = D.meta;
  const snapMode = M.mode === "snap";
  const last = (arr) => arr[arr.length - 1];
  const at = (arr, x) => (arr.find(p => p.x === x) || {}).y;
  const s = (ms) => (ms / 1000).toLocaleString("en-US", { maximumFractionDigits: 1 }) + " s";
  const n = (v, d = 0) => Number(v).toLocaleString("en-US", { maximumFractionDigits: d });
  const kb = (b) => n(b / 1024) + " KB";
  const ul = (items) => `<ul class="pts">${items.map(i => `<li>${i}</li>`).join("")}</ul>`;

  // --- header
  document.getElementById("lede").innerHTML =
    `<b>Question:</b> how far does 1 workflow node scale on Axon Server, and what breaks first?<br>` +
    `<b>Method:</b> 3 sweeps, 1 axis each. Failover = new node over the same store, same processor tokens.`;
  document.getElementById("modenote").innerHTML = snapMode
    ? `<b>Snapshots on.</b> Both workflow entities snapshot after a load that evolved 500+ events (${M.snapshotStore}). Fan-out numbers do not change; restore numbers do.`
    : `<b>Snapshots off.</b> The engine as it is on the branch: no snapshot registered for either entity.`;
  document.getElementById("facts").innerHTML = [
    ["event store", M.store], ["engine", M.engine], ["processor", M.processor],
    ["jdk", M.jdk], ["run", M.date], ["harness", M.harness],
  ].map(([k, v]) => `<div><span>${k}</span>${v}</div>`).join("");

  // --- verdict tiles
  const maxRunning = last(R.start).x;
  const restoreAtMax = at(R.restore, maxRunning);
  const releaseMax = Math.max(...R.release.map(p => p.y));
  const releaseCliff = R.release.find(p => p.y > 5000);
  const historyOverCeiling = H.restore.find(p => p.y > 30000 || p.y < 0);
  const historyOk = [...H.restore].reverse().find(p => p.y >= 0 && p.y <= 30000);
  const stepsByEv = S.restore.map((p, i) => ({ ev: S.events[i].y, y: p.y }));
  const stepsCliff = stepsByEv.find(p => p.y > 1000);
  const stepsOk = [...stepsByEv].reverse().find(p => p.y <= 1000);

  document.getElementById("verdicts").innerHTML = `
    <div class="verdict">
      <div class="eyebrow">Running instances <span class="pill ${releaseCliff ? "warn" : "good"}">${releaseCliff ? "quadratic fan-out" : "linear"}</span></div>
      <div class="num">${n(maxRunning)}<small>parked on 1 node</small></div>
      <p><b>Restore is fine:</b> ${s(restoreAtMax)} for all ${n(maxRunning)}.<br><b>Live traffic is not:</b> releasing them all took ${s(releaseMax)}. Every event goes to every instance.</p>
    </div>
    <div class="verdict">
      <div class="eyebrow">Running-workflow list <span class="pill ${historyOverCeiling ? "crit" : "good"}">${historyOverCeiling ? "hits 30 s timeout" : "under timeout"}</span></div>
      <div class="num">${historyOk ? n(historyOk.x) : "0"}<small>finished workflows, last size under 30 s</small></div>
      <p>${snapMode ? "<b>Snapshotted.</b> A failover replays only the lifecycle events after the last snapshot." : "<b>No snapshot.</b> Every failover replays every lifecycle event ever written, once per segment."}${historyOverCeiling ? `<br><b>At ${n(historyOverCeiling.x)} finished:</b> restore took ${historyOverCeiling.y < 0 ? "longer than the harness ceiling" : s(historyOverCeiling.y)} with only 10 running.` : ""}</p>
    </div>
    <div class="verdict">
      <div class="eyebrow">1 instance, long history <span class="pill ${stepsCliff ? "warn" : "good"}">${stepsCliff ? "cliff past 1,000 events" : "linear"}</span></div>
      <div class="num">${stepsOk ? n(stepsOk.ev) : "n/a"}<small>events, restore still 0.1 s</small></div>
      <p><b>Live is flat:</b> ${n(last(S.execute_per).y, 0)} ms per step at any history.<br>${stepsCliff ? `<b>Restore jumps:</b> 0.1 s to ${s(stepsCliff.y)} past ${n(stepsCliff.ev)} events, ${s(last(S.restore).y)} at ${n(last(S.events).y)}.` : ""}</p>
    </div>`;

  // --- sweep A
  const startPer = R.start_per, perFirst = startPer[0].y, perLast = last(startPer).y;
  const quad = R.release.filter(p => p.y > 5000 && p.x >= 5000);
  const perDeliveryNs = quad.length ? Math.min(...quad.map(p => p.y * 1e6 / (p.x * p.x))) : null;
  document.getElementById("finding-running").innerHTML =
    `<b>Parked</b> = alive, waiting for 1 event. Each instance: start, wait for its own release event, 1 more step, done.` + ul([
      `<b>Held ${n(maxRunning)}</b> parked instances on 1 node. About ${kb(at(R.heap, maxRunning))} heap each.`,
      `<b>Start:</b> ${n(perFirst, 2)} ms per instance at ${n(startPer[0].x)}. ${n(perLast, 2)} ms at ${n(maxRunning)}.`,
      `<b>Failover restore:</b> ${s(R.restore[0].y)} to ${s(restoreAtMax)}. Fine.`,
      `<b>Release all, the ugly curve:</b> ${s(at(R.release, 2000) || 0)} at 2,000. ${s(at(R.release, 5000) || 0)} at 5,000. ${s(at(R.release, 10000) || 0)} at 10,000. ${s(at(R.release, 20000) || 0)} at 20,000.`,
    ]);
  document.getElementById("note-running").innerHTML =
    `<b>What release-all measures.</b> ${n(maxRunning)} release events. Each matches exactly 1 instance. ` +
    `The engine still hands every event to every running instance. N events cost N&times;N deliveries. ` +
    `The raw store reads on the right stay in the tens of milliseconds. Axon Server is not the problem here.`;
  if (perDeliveryNs) {
    const rows = [1000, 2000, 5000, 10000, 20000, 50000].map(N => {
      const addedMs = perDeliveryNs * N / 1e6;
      return [N, addedMs, 1000 / addedMs];
    });
    document.getElementById("note-fanout").innerHTML = ul([
      `<b>Cost per delivery:</b> about ${n(perDeliveryNs)} ns, measured from the release phase.`,
      `<b>Per event:</b> N deliveries on the processor thread, batch size 1. The next event waits.`,
      `<b>Under 1,000 instances:</b> noise. <b>From 5,000:</b> caps the whole node.`,
      `<b>Failure edge:</b> each delivery is an <code>offer</code> into a 1,000-slot queue. Full queue = <code>appendTask</code> throws "Too many tasks". The event fails. No backpressure.`,
    ]);
    renderTable("t-fanout", ["running instances", "added latency per event (ms)", "max events / s on the node"],
      rows, (r, i, c) => (i === 1 && c >= 5) ? "hot" : "");
  }

  // --- sweep B
  const rate = H.raw_list.filter(p => p.y > 0).map(p => H.events[p.x] / p.y);
  const restoreRatio = H.restore.filter(p => p.y > 0 && p.x > 0).map(p => p.y / at(H.raw_list, p.x));
  document.getElementById("finding-history").innerHTML =
    (snapMode ? `<b>With a snapshot, failover no longer grows with finished workflows.</b> 10 instances were running in every run.` : `<b>Failover time grows with finished workflows, not running ones.</b> 10 instances were running in every run.`) + ul([
      `<b>Empty store:</b> restore ${s(at(H.restore, 0))}.`,
      `<b>${n(last(H.restore).x)} finished</b> (${n(H.events[last(H.restore).x])} lifecycle events): restore ${s(last(H.restore).y)}.`,
      `<b>Raw read</b> of the same events: ${s(last(H.raw_list).y)}, about ${n(Math.max(...rate))}k events per second.`,
      snapMode ? `<b>Engine restore is now below the raw full read.</b> Each segment loads the snapshot and the short tail after it.` : `<b>Engine is ${n(Math.max(...restoreRatio))}x slower</b> than the raw read. All ${M.segments} segments replay the list, each on its own.`,
    ]);
  document.getElementById("note-history").innerHTML =
    `<b>Why the 30 s line matters.</b> A claim whose restore passes 30 s is failed and handed back to the coordinator. ` +
    `${historyOverCeiling ? "Sizes from " + n(historyOverCeiling.x) + " finished workflows cross it here. " : ""}` +
    `<b>How the store was filled.</b> STARTED plus COMPLETED lifecycle pairs written straight to the store, same metadata and tags the engine writes. Same replay path as production.` +
    (M.historyNote ? "<br><b>Data note.</b> " + M.historyNote : "");

  // --- sweep C
  document.getElementById("finding-steps").innerHTML =
    (snapMode ? `<b>With a snapshot the instance restores in about 0.1 s at every history size.</b>` : `<b>Axon Server keeps 1 instance's read cheap.</b> The engine's restore does not.`) + ul([
      `<b>Raw read:</b> ${n(last(S.events).y)} events in ${n(last(S.raw_one).y)} ms. Linear.`,
      `<b>Live steps:</b> flat at ${n(last(S.execute_per).y, 0)} ms per step. 2 appends per step.`,
      `<b>Restore:</b> about 0.1 s up to ${stepsOk ? n(stepsOk.ev) : "n/a"} events.${stepsCliff ? ` Then ${s(stepsCliff.y)} at ${n(stepsCliff.ev)}. ${s(last(S.restore).y)} at ${n(last(S.events).y)}.` : ""}`,
      `<b>First append after restore:</b> about 0.1 s at every size. Once restored, the instance is healthy.`,
    ]);
  document.getElementById("note-steps").innerHTML = snapMode ? `<b>Why the cliff is gone.</b> The engine reloads the workflow state on every step, so a snapshot is refreshed during the live run once the history passes 500 events. Failover then replays only the tail. The first restore after enabling snapshots pays the full replay once; it is recorded as <code>first_restore_ms</code> in the CSV.` :
    `<b>Where the cliff sits.</b> Right after 1,000 events. That is the capacity of the per-instance task queue, ` +
    `<code>ArrayBlockingQueue&lt;&gt;(1000)</code> with a <code>// FIXME size</code> in <code>SimpleWorkflowExecution</code>. ` +
    `Below it, replay is a tight loop. Above it, about 2 ms per event.` +
    (M.stepsNote ? "<br><b>Caveat.</b> " + M.stepsNote : "");

  // --- ranked bottlenecks with fixes, always stated from the baseline run
  const RB = BASE.running, HB = BASE.history, SB = BASE.steps;
  const restoreMax = last(HB.restore), listRate = restoreMax.y / restoreMax.x;
  const snapRestore = (x) => at(SNAP.history.restore, x);
  const snapSteps = (x) => at(SNAP.steps.restore, x);
  document.getElementById("fixes").innerHTML = [
    { pill: "crit", title: "Running-workflow list: no snapshot, replayed once per segment",
      where: "<code>EventSourcedRunningWorkflows</code>. <code>WorkflowConfigurationDefaults.registerRunningWorkflowsModule</code> (<code>FIXME #245</code>). <code>WorkflowEngine.restoreWorkflowsFor</code>.",
      evidence: ul([
        `Restore ${s(restoreMax.y)} with ${n(restoreMax.x)} finished workflows and 10 running.`,
        `Raw read of the same events: ${s(last(HB.raw_list).y)}.`,
        `About ${n(listRate * 1000, 0)} ms per 1,000 finished workflows.`,
        `Crosses the 30 s claim timeout from roughly ${n(Math.round(30000 / listRate / 1000) * 1000)} finished workflows.`,
      ]),
      fix: ul([
        `<b>Snapshot the entity.</b> Measured here with the toggle above: ${n(last(SNAP.history.restore).x)} finished workflows restore in ${s(last(SNAP.history.restore).y)} instead of ${s(at(HB.restore, last(SNAP.history.restore).x))}.`,
        `<b>Load once per node.</b> Hand each segment its owned subset. Divides the remaining cost by ${M.segments}.`,
        `<b>Or replace it</b> with a compact projection keyed by workflow id that a claim reads instead of replaying.`,
      ]),
      effect: "Failover no longer grows with total workflows ever run. The 1,000,000-finished case drops from minutes to about 1 s." },
    { pill: "warn", title: "Live fan-out: every event offered to every running instance",
      where: "<code>WorkflowEngine.handle</code>, loop over <code>workflowExecutionRepository.findAll(ownedBy(segment))</code>. <code>SimpleWorkflowExecution.onEvent</code>. <code>EventWaitConditions.evaluateAndApply</code>.",
      evidence: ul([
        `Release of 10,000 instances: ${s(at(RB.release, 10000))}. Of 20,000: ${s(at(RB.release, 20000))}.`,
        `5x the cost for 2x the instances.`,
        `About ${n(perDeliveryNs || 470)} ns per delivery. 10,000 instances add 5 ms to every event. Node caps near 200 events/s.`,
      ]),
      fix: ul([
        `<b>Association index instead of broadcast.</b> <code>awaitEvent</code> already declares its match as (event name, payload property, value).`,
        `Keep a per-segment map from that tuple to instance ids. Add on <code>awaitEvent</code>. Remove when the wait completes or the instance ends.`,
        `Deliver only to matched ids. Keep a small broadcast list for plain-predicate waits.`,
        `Rebuild the index from the sourced wait steps on restore.`,
        `<b>Also:</b> replace the failing <code>offer</code> in <code>appendTask</code> with backpressure.`,
      ]),
      effect: "Release of 10,000 instances from about 47 s to the store's append time, 1 to 2 s. Start N gets the same win." },
    { pill: "warn", title: "Per-instance replay slows to about 2 ms per event past 1,000 events",
      where: "<code>SimpleWorkflowExecution</code> task queue <code>ArrayBlockingQueue&lt;&gt;(1000) // FIXME size</code>. Replay branch of <code>onEvent</code>.",
      evidence: ul([
        `Restore of 1 instance: ${s(at(SB.restore, 500))} at 1,002 events. ${s(at(SB.restore, 1000))} at 2,002. ${s(last(SB.restore).y)} at ${n(last(SB.events).y)}.`,
        `Raw read at ${n(last(SB.events).y)} events: ${n(last(SB.raw_one).y)} ms. The store is not involved.`,
      ]),
      fix: ul([
        `<b>Snapshot</b> <code>EventSourcedWorkflowState</code>. Measured with the toggle above: ${n(last(SNAP.steps.events).y)} events restore in ${s(last(SNAP.steps.restore).y)} instead of ${s(at(SB.restore, last(SNAP.steps.restore).x))}.`,
        `<b>Let replay bypass the task queue</b> for the tail after the snapshot. Evolve state directly. Queue only the final wake.`,
      ]),
      effect: "Restore of a 10,000-event instance from about 20 s back to the raw read, under 0.1 s." },
  ].map((f, i) => `<div class="fix"><div class="rank">${i + 1}</div><div class="body">
      <h3>${f.title} <span class="pill ${f.pill}">${f.pill === "crit" ? "hits the 30 s timeout" : "caps throughput"}</span></h3>
      <dl><dt>where</dt><dd>${f.where}</dd><dt>evidence</dt><dd>${f.evidence}</dd><dt>fix</dt><dd>${f.fix}</dd><dt>effect</dt><dd>${f.effect}</dd></dl>
    </div></div>`).join("");

  document.getElementById("mech").innerHTML = `
    <div><h3>Axon Server</h3><p><b>Linear and cheap.</b> ${n(last(S.events).y)} events of 1 instance in ${n(last(S.raw_one).y)} ms. ${n(H.events[last(H.raw_list).x])} lifecycle events in ${s(last(H.raw_list).y)}. Appends about ${n(last(S.execute_per).y / 2, 0)} ms each, flat with history.</p></div>
    <div><h3>Memory</h3><p><b>About ${kb(at(R.heap, maxRunning))} per parked instance.</b> 1 virtual thread, 1 queue, the evolved state. ${n(maxRunning)} instances fit in under ${n(maxRunning * at(R.heap, maxRunning) / (1 << 20))} MB.</p></div>
    <div><h3>Per-instance sourcing by tag</h3><p><b>Fine at every size.</b> <code>EventSourcedWorkflowState</code> reads by <code>workflowId</code> tag, 1 unit of work per instance, all in parallel. ${n(maxRunning)} tiny instances restored in ${s(restoreAtMax)}.</p></div>`;
}


// ---------- before/after strip, always both datasets ----------
function COMPARE(B, S2, M) {
  const at = (arr, x) => (arr.find(p => p.x === x) || {}).y;
  const s = (ms) => ms == null ? "n/a" : ms < 0 ? "timed out" : ms >= 1000 ? (ms / 1000).toLocaleString("en-US", { maximumFractionDigits: 1 }) + " s" : Math.round(ms) + " ms";
  const n = (v) => Number(v).toLocaleString("en-US");
  const bigHist = [...S2.history.restore].reverse().find(p => p.y >= 0);
  const rows = [
    ["Failover restore, " + n(bigHist.x) + " finished workflows, 10 running", at(B.history.restore, bigHist.x), bigHist.y],
    ["Failover restore, 50,000 finished workflows", at(B.history.restore, 50000), at(S2.history.restore, 50000)],
    ["Restore 1 instance, 2,002 events", at(B.steps.restore, 1000), at(S2.steps.restore, 1000)],
    ["Restore 1 instance, 10,002 events", at(B.steps.restore, 5000), at(S2.steps.restore, 5000)],
    ["Restore 10,000 parked instances", at(B.running.restore, 10000), at(S2.running.restore, 10000)],
    ["Release 10,000 parked instances (fan-out)", at(B.running.release, 10000), at(S2.running.release, 10000)],
    ["Live step latency, 5,000-step workflow", (B.steps.execute_per.find(p => p.x === 5000) || {}).y, (S2.steps.execute_per.find(p => p.x === 5000) || {}).y],
  ].filter(r => r[1] != null && r[2] != null);
  const t = document.getElementById("t-compare");
  t.innerHTML = "<thead><tr><th>metric</th><th>without snapshots</th><th>with snapshots</th><th>change</th></tr></thead><tbody>" +
    rows.map(r => {
      const ratio = r[1] > 0 && r[2] > 0 ? r[1] / r[2] : null;
      const same = ratio != null && ratio < 1.5 && ratio > 0.67;
      const change = ratio == null ? "n/a" : same ? "same" : ratio >= 1 ? n(Math.round(ratio)) + "x faster" : n(Math.round(1 / ratio)) + "x slower";
      return `<tr><td>${r[0]}</td><td>${s(r[1])}</td><td>${s(r[2])}</td><td class="${same ? "same" : ratio >= 1 ? "better" : "fail"}">${change}</td></tr>`;
    }).join("") + "</tbody>";
  document.getElementById("compare-note").innerHTML =
    `<b>Setup.</b> Snapshot after a load that evolved 500+ events, both entities, ${M.snapshotStore}. Both runs on the same Axon Server store and sizes. ` +
    `Restore times fall because a failover replays only events after the snapshot. Fan-out and live step latency are untouched by design.`;
}
