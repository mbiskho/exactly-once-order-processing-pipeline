const root = document.querySelector("#app");

const esc = (value) => String(value).replace(/[&<>"']/g, (char) => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
}[char]));
const money = (cents) => new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(cents / 100);
const newPipeline = () => ({
  orders: [],
  events: [],
  broker: new Map(),
  applied: new Map(),
  idempotency: new Map(),
  seen: new Set(),
  deadLetters: [],
  sequence: new Map(),
  networkBlocked: false,
  duplicateRequests: 0,
  duplicateDeliveries: 0,
  nextID: 1,
});
let system = newPipeline();
let selectedKey = "customer-42";
let formValues = { key: selectedKey, idempotency: "checkout-1042", amount: "25.99" };
let notice = { text: "Ready. Submit an order, then publish its outbox event.", kind: "success" };
let chaosReport = null;
let publishFault = "none";
let consumeFault = "none";

function byID(state, id) {
  return state.events.find((event) => event.id === id);
}

function submitOrder(state, key, idempotencyKey, amountCents) {
  if (!key.trim() || !idempotencyKey.trim() || !Number.isSafeInteger(amountCents) || amountCents <= 0) {
    return { error: "Provide a partition key, idempotency key, and positive amount." };
  }
  const prior = state.idempotency.get(idempotencyKey);
  if (prior) {
    if (prior.key !== key || prior.amountCents !== amountCents) {
      return { error: "That idempotency key already belongs to a different order payload." };
    }
    state.duplicateRequests += 1;
    return { order: prior, duplicate: true };
  }
  const sequence = (state.sequence.get(key) || 0) + 1;
  state.sequence.set(key, sequence);
  const order = { id: `ord-${String(state.nextID).padStart(6, "0")}`, key, idempotencyKey, sequence, amountCents };
  state.nextID += 1;
  state.orders.push(order);
  state.idempotency.set(idempotencyKey, order);
  state.events.push({ id: `evt-${order.id}`, order, state: "outbox-pending", publishTry: 0, deliveryTry: 0 });
  return { order, duplicate: false };
}

function firstUnapplied(state, key) {
  return state.events.find((event) => event.order.key === key && event.state !== "consumer-applied");
}

function publishNext(state, key, fault = "none") {
  if (state.networkBlocked) return { error: "Network partition: publisher cannot reach the broker." };
  const event = firstUnapplied(state, key);
  if (!event || event.state !== "outbox-pending") return { error: "No publishable outbox event for this key." };
  event.publishTry += 1;
  if (fault === "broker-before") return { error: "Broker stopped before accepting the event; the outbox row remains pending." };
  const queue = state.broker.get(key) || [];
  queue.push(event.id);
  state.broker.set(key, queue);
  if (fault === "broker-after") return { error: "Broker accepted the event but the acknowledgement was lost; retry is safe." };
  event.state = "broker-published";
  return { event };
}

function consumeNext(state, key, fault = "none") {
  if (state.networkBlocked) return { error: "Network partition: consumer cannot reach the broker." };
  const queue = state.broker.get(key) || [];
  if (!queue.length) return { error: "No broker delivery is waiting for this key." };
  const id = queue[0];
  const event = byID(state, id);
  if (!event) return { error: `Broker points to missing event ${id}.` };
  event.deliveryTry += 1;
  if (state.seen.has(id)) {
    state.duplicateDeliveries += 1;
    queue.shift();
    return { event, duplicate: true };
  }
  const applied = state.applied.get(key) || [];
  if (event.order.sequence !== applied.length + 1) {
    return { error: `Ordering guard blocked sequence ${event.order.sequence}; expected ${applied.length + 1}.` };
  }
  if (fault === "consumer-before") return { error: "Consumer stopped before commit; message remains available for redelivery." };
  state.seen.add(id);
  applied.push(id);
  state.applied.set(key, applied);
  event.state = "consumer-applied";
  if (fault === "consumer-after") return { event, error: "Commit succeeded but consumer acknowledgement was lost; redelivery will deduplicate." };
  queue.shift();
  return { event, duplicate: false };
}

function quarantineNext(state, key, reason = "Operator quarantined poison event") {
  const queue = state.broker.get(key) || [];
  if (!queue.length) return { error: "No broker delivery is available to quarantine." };
  const id = queue[0];
  if (state.seen.has(id)) return { error: "An already-committed event cannot be quarantined." };
  const event = byID(state, id);
  if (!event) return { error: `Broker points to missing event ${id}.` };
  event.state = "dead-lettered";
  state.deadLetters.push({ eventId: id, reason, at: new Date().toISOString() });
  for (let index = queue.length - 1; index >= 0; index -= 1) {
    if (queue[index] === id) queue.splice(index, 1);
  }
  return { event };
}

function replay(state, id) {
  const event = byID(state, id);
  if (!event || event.state !== "dead-lettered") return { error: "Select a dead-lettered event to replay." };
  event.state = "outbox-pending";
  state.deadLetters = state.deadLetters.filter((item) => item.eventId !== id);
  return { event };
}

function verify(state) {
  const seenApplied = new Set();
  for (const [key, ids] of state.applied) {
    for (let index = 0; index < ids.length; index += 1) {
      const event = byID(state, ids[index]);
      if (seenApplied.has(ids[index])) return `Duplicate business effect: ${ids[index]}`;
      seenApplied.add(ids[index]);
      if (!event || event.order.key !== key || event.order.sequence !== index + 1) return `Per-key order invariant failed for ${key}.`;
      if (!state.seen.has(ids[index]) || event.state !== "consumer-applied") return `Dedup/effect state mismatch: ${ids[index]}`;
    }
  }
  if (state.orders.length !== state.events.length) return "Transactional outbox coverage mismatch.";
  for (const event of state.events) {
    if (!["outbox-pending", "broker-published", "consumer-applied", "dead-lettered"].includes(event.state)) {
      return `Unknown event state: ${event.state}`;
    }
    if (state.seen.has(event.id) !== seenApplied.has(event.id)) return `Dedup store mismatch: ${event.id}`;
  }
  return "";
}

function makeChaosReport() {
  let randomSeed = 42;
  const random = (max) => {
    randomSeed = (randomSeed * 1664525 + 1013904223) >>> 0;
    return randomSeed % max;
  };
  const report = { runs: 20, orders: 0, faultsInjected: 0, suppressedRedeliveries: 0, failures: [] };
  for (let run = 0; run < 20; run += 1) {
    const candidate = newPipeline();
    for (let index = 0; index < 12; index += 1) {
      submitOrder(candidate, `account-${index % 3}`, `run-${run}-order-${index}`, 100 + index);
    }
    for (let turn = 0; turn < 2000 && candidate.events.filter((event) => event.state === "consumer-applied").length < 12; turn += 1) {
      for (const key of ["account-0", "account-1", "account-2"]) {
        if (random(35) === 0) {
          candidate.networkBlocked = true;
          const blocked = publishNext(candidate, key);
          candidate.networkBlocked = false;
          if (!blocked.error) report.failures.push(`Run ${run + 1}: network partition was not enforced.`);
          report.faultsInjected += 1;
        }
        const event = firstUnapplied(candidate, key);
        if (!event) continue;
        if (event.state === "outbox-pending") {
          let fault = "none";
          if (random(8) === 0) {
            fault = random(2) === 0 ? "broker-before" : "broker-after";
            report.faultsInjected += 1;
          }
          publishNext(candidate, key, fault);
        } else if (event.state === "broker-published") {
          let fault = "none";
          if (random(8) === 0) {
            fault = random(2) === 0 ? "consumer-before" : "consumer-after";
            report.faultsInjected += 1;
          }
          consumeNext(candidate, key, fault);
        }
      }
      const problem = verify(candidate);
      if (problem) report.failures.push(`Run ${run + 1}: ${problem}`);
    }
    if (candidate.events.filter((event) => event.state === "consumer-applied").length !== 12) {
      report.failures.push(`Run ${run + 1}: pipeline did not drain.`);
    }
    report.orders += candidate.orders.length;
    report.suppressedRedeliveries += candidate.duplicateDeliveries;
  }
  return report;
}

function panel(title, subtitle, body) {
  return `<section class="panel"><header class="panel-header"><div><h2>${esc(title)}</h2><p>${esc(subtitle)}</p></div></header><div class="panel-body">${body}</div></section>`;
}

function render() {
  const stats = {
    orders: system.orders.length,
    applied: system.seen.size,
    pending: system.events.filter((event) => event.state === "outbox-pending").length,
    broker: [...system.broker.values()].reduce((sum, queue) => sum + queue.length, 0),
    dlq: system.deadLetters.length,
  };
  const eventRows = [...system.events].reverse().map((event) => `<tr>
    <td class="mono">${esc(event.order.id)}</td><td>${esc(event.order.key)} <span class="smallprint">#${event.order.sequence}</span></td>
    <td class="mono">${esc(event.id)}</td><td>${money(event.order.amountCents)}</td>
    <td><span class="state ${event.state === "dead-lettered" ? "dead" : event.state === "outbox-pending" ? "pending" : ""}">${esc(event.state)}</span></td>
    <td class="smallprint">${event.publishTry} / ${event.deliveryTry}</td></tr>`).join("");
  const dlqRows = system.deadLetters.length ? system.deadLetters.map((item) => {
    const event = byID(system, item.eventId);
    return `<div class="mini"><span>${esc(item.eventId)} · ${esc(item.reason)}</span><strong>${event ? esc(event.order.key) : "Unknown key"}</strong><button class="btn secondary" data-action="replay" data-id="${esc(item.eventId)}" style="float:right">Replay</button></div>`;
  }).join("") : '<div class="empty">No quarantined events.</div>';
  root.innerHTML = `
    <header class="topbar"><a class="brand" href="./index.html"><span class="brand-mark">OP</span><span>ORDER PIPELINE LAB</span></a><nav class="top-links"><a href="./README.md">Project docs</a><span class="pill">${system.networkBlocked ? "network partition" : "sandbox active"}</span></nav></header>
    <main class="page">
      <p class="eyebrow">DISTRIBUTED SYSTEMS · FAILURE EXERCISE</p>
      <h1>Exactly-once is an application invariant.</h1>
      <p class="lead">Orders and outbox rows are committed together. The publisher may deliver more than once; the consumer deduplicates in the same transaction as its business effect, while per-key sequence checks prevent reordering.</p>
      <div class="notice"><strong>Browser mode:</strong> this interactive lab is an in-memory simulation. A separate PostgreSQL + Kafka-compatible stack is available with Docker Compose; this page does not send requests to it. <a href="http://localhost:8080/health">Check local API health</a>.</div>
      <section class="metrics">
        <div class="metric"><div class="metric-label">Orders accepted</div><div class="metric-value">${stats.orders}</div></div>
        <div class="metric"><div class="metric-label">Effects committed</div><div class="metric-value good">${stats.applied}</div></div>
        <div class="metric"><div class="metric-label">Outbox pending</div><div class="metric-value ${stats.pending ? "warn" : ""}">${stats.pending}</div></div>
        <div class="metric"><div class="metric-label">Broker deliveries</div><div class="metric-value">${stats.broker}</div></div>
        <div class="metric"><div class="metric-label">Dead-lettered</div><div class="metric-value ${stats.dlq ? "warn" : ""}">${stats.dlq}</div></div>
      </section>
      <div class="layout">
        <div class="stack">
          ${panel("1 · Accept an order", "Idempotency key prevents a retry from creating another order or outbox record.", `
            <form id="order-form">
              <label class="label" for="order-key">Partition key (customer/account)</label><input id="order-key" name="key" value="${esc(formValues.key)}" maxlength="80" required>
              <label class="label" for="idempotency-key">Idempotency key</label><input id="idempotency-key" name="idempotency" value="${esc(formValues.idempotency)}" maxlength="100" required>
              <label class="label" for="amount">Amount (USD)</label><input id="amount" name="amount" type="number" min="0.01" step="0.01" value="${esc(formValues.amount)}" required>
              <button class="btn full" type="submit">Commit order + outbox row</button>
            </form>
            <div class="result ${notice.kind}" role="status">${esc(notice.text)}</div>
            <p class="smallprint" style="margin:12px 0 0">Submit the same key and payload again to test deduplication. Reusing a key with different contents is rejected.</p>
          `)}
          ${panel("2 · Inject delivery failures", "Faults are injected at the boundary where retry semantics matter.", `
            <label class="label" for="publish-fault">Publisher fault</label><select id="publish-fault">
              <option value="none" ${publishFault === "none" ? "selected" : ""}>No fault</option><option value="broker-before" ${publishFault === "broker-before" ? "selected" : ""}>Broker dies before accepting</option><option value="broker-after" ${publishFault === "broker-after" ? "selected" : ""}>Publish accepted; acknowledgement lost</option>
            </select>
            <button class="btn full" data-action="publish">Publish next outbox event for selected key</button>
            <label class="label" for="consume-fault">Consumer fault</label><select id="consume-fault">
              <option value="none" ${consumeFault === "none" ? "selected" : ""}>No fault</option><option value="consumer-before" ${consumeFault === "consumer-before" ? "selected" : ""}>Consumer dies before transaction commit</option><option value="consumer-after" ${consumeFault === "consumer-after" ? "selected" : ""}>Commit succeeds; acknowledgement lost</option>
            </select>
            <button class="btn secondary full" data-action="consume">Consume next broker delivery</button>
            <div class="button-row">
              <button class="btn ${system.networkBlocked ? "" : "warn"}" data-action="partition">${system.networkBlocked ? "Heal network partition" : "Partition publisher/consumer"}</button>
              <button class="btn danger" data-action="quarantine">Quarantine next delivery</button>
            </div>
          `)}
        </div>
        <div class="stack">
          ${panel("Pipeline topology", "Per-key sequencing; consumer effect and dedup record commit atomically.", `
            <div class="flow">
              <div class="flow-step"><span>01 · command</span><strong>Order API</strong><em>idempotency key</em></div>
              <div class="flow-step"><span>02 · same DB tx</span><strong>Orders + outbox</strong><em>no dual write</em></div>
              <div class="flow-step"><span>03 · outbox relay</span><strong>Broker publish</strong><em>at-least-once</em></div>
              <div class="flow-step"><span>04 · consumer tx</span><strong>Dedup + effect</strong><em>same transaction</em></div>
              <div class="flow-step"><span>05 · assert</span><strong>Invariant checks</strong><em>loss · dup · order</em></div>
            </div>
            <div class="button-row">
              <button class="btn secondary" data-action="chaos">Run 20 chaos trials</button>
              <button class="btn secondary" data-action="reset">Reset lab</button>
            </div>
            <p class="smallprint" style="margin:12px 0 0"><a href="./reports/durable-system-flow.png">Open durable-system flowchart</a> · <a href="./sql/schema.sql">View SQL transaction contract</a> · <a href="http://localhost:8080/health">Check API health</a></p>
            <div class="result ${chaosReport?.failures.length ? "error" : "success"}" role="status">${chaosReport ? chaosReport.failures.length ? chaosReport.failures.slice(0, 3).map(esc).join("<br>") : `20 runs · ${chaosReport.orders} accepted · ${chaosReport.faultsInjected} injected faults · ${chaosReport.suppressedRedeliveries} safe redeliveries suppressed · 0 lost · 0 duplicate effects · 0 ordering violations.` : "Chaos suite not run in this browser session."}</div>
          `)}
          ${panel("Outbox and broker state", "Publish retries can duplicate transport deliveries; business effects remain idempotent.", `
            <div class="table-wrap"><table><thead><tr><th>Order</th><th>Key / seq</th><th>Event</th><th>Amount</th><th>State</th><th>Publish / delivery attempts</th></tr></thead>
            <tbody>${eventRows || '<tr><td colspan="6" class="empty">No orders yet. Submit an order to create the atomic order/outbox pair.</td></tr>'}</tbody></table></div>
          `)}
          ${panel("Dead-letter queue and replay", "Quarantine removes a poison event from active delivery without advancing its key sequence. Replay puts it back through the outbox.", dlqRows)}
          ${panel("Run the executable model", "From this project folder:", `
            <div class="command">go test -race ./...</div>
            <div class="command" style="margin-top:8px">go run ./cmd/pipeline demo</div>
            <div class="command" style="margin-top:8px">go run ./cmd/pipeline chaos -runs 20 -seed 42</div>
            <div class="command" style="margin-top:8px">go run ./cmd/pipeline replay</div>
            <div class="command" style="margin-top:8px">go run ./cmd/pipeline dlq</div>
            <p class="smallprint" style="margin:12px 0 0">The model is intentionally dependency-free and repeatable. See <a href="./reports/failure-modes.md">failure modes</a> and <a href="./reports/chaos-test-results.md">chaos report</a>.</p>
          `)}
        </div>
      </div>
      <p class="footer">A portfolio reference implementation, not a production broker or database deployment. Zero loss/duplicate/order-violation claims refer only to the deterministic in-memory invariant suite.</p>
    </main>`;
}

root.addEventListener("submit", (event) => {
  if (event.target.id !== "order-form") return;
  event.preventDefault();
  const form = new FormData(event.target);
  formValues = {
    key: String(form.get("key")),
    idempotency: String(form.get("idempotency")),
    amount: String(form.get("amount")),
  };
  selectedKey = String(form.get("key")).trim();
  const idempotencyKey = String(form.get("idempotency")).trim();
  const amountCents = Math.round(Number(form.get("amount")) * 100);
  const result = submitOrder(system, selectedKey, idempotencyKey, amountCents);
  notice = result.error
    ? { text: result.error, kind: "error" }
    : result.duplicate
      ? { text: `Retry suppressed. Existing ${result.order.id} remains the only accepted order for this key.`, kind: "success" }
      : { text: `Accepted ${result.order.id} · sequence ${result.order.sequence} for ${money(result.order.amountCents)}. Order and outbox event committed together.`, kind: "success" };
  render();
});

root.addEventListener("click", (event) => {
  const button = event.target.closest("[data-action]");
  if (!button) return;
  publishFault = document.querySelector("#publish-fault")?.value || publishFault;
  consumeFault = document.querySelector("#consume-fault")?.value || consumeFault;
  const action = button.dataset.action;
  let result;
  if (action === "publish") result = publishNext(system, selectedKey, publishFault);
  if (action === "consume") result = consumeNext(system, selectedKey, consumeFault);
  if (action === "quarantine") result = quarantineNext(system, selectedKey);
  if (action === "replay") result = replay(system, button.dataset.id);
  if (action === "partition") {
    system.networkBlocked = !system.networkBlocked;
    notice = { text: system.networkBlocked ? "Network partition enabled. Publish and consume operations are blocked; order submission still writes to the local outbox." : "Network partition healed. Pending outbox events can resume.", kind: system.networkBlocked ? "error" : "success" };
    render();
    return;
  }
  if (action === "chaos") {
    chaosReport = makeChaosReport();
    notice = chaosReport.failures.length
      ? { text: `Chaos run found ${chaosReport.failures.length} invariant or recovery failure(s).`, kind: "error" }
      : { text: `20 deterministic chaos runs passed: ${chaosReport.orders} orders, ${chaosReport.faultsInjected} injected failures.`, kind: "success" };
    render();
    return;
  }
  if (action === "reset") {
    system = newPipeline();
    chaosReport = null;
    formValues = { key: "customer-42", idempotency: "checkout-1042", amount: "25.99" };
    selectedKey = formValues.key;
    publishFault = "none";
    consumeFault = "none";
    notice = { text: "Lab state reset. No persisted data was changed.", kind: "success" };
    render();
    return;
  }
  if (result) {
    notice = result.error
      ? { text: result.error, kind: "error" }
      : { text: result.duplicate ? `${result.event.id} redelivery deduplicated; no second business effect.` : `${result.event.id} ${action === "publish" ? "published" : action === "replay" ? "returned to the outbox" : action === "quarantine" ? "quarantined" : "committed"}.`, kind: "success" };
    if (result.error && result.event) notice = { text: result.error, kind: "error" };
  }
  const invariantError = verify(system);
  if (invariantError) notice = { text: `INVARIANT FAILED: ${invariantError}`, kind: "error" };
  render();
});

render();
