import React, { useState } from 'react';
import { api } from './api.js';

export default function Simulate({ status }) {
  const [busy, setBusy] = useState(null);
  const [log, setLog] = useState([]);
  const [corruptCount, setCorruptCount] = useState(3);
  const [dripCount, setDripCount] = useState(20);

  function push(text) {
    setLog((l) => [{ t: new Date().toLocaleTimeString(), text }, ...l].slice(0, 20));
  }

  async function injectCorrupt() {
    setBusy('corrupt');
    try {
      const n = Math.max(1, parseInt(corruptCount, 10) || 3);
      const r = await api.simulateCorrupt(n);
      push(`Inserted ${n} corrupt row(s) (amount="N/A"): ids ${r.ids.join(', ')}. Watch the DLQ count on the Status tab.`);
    } catch (e) {
      push(`Failed: ${e.message}`);
    } finally { setBusy(null); }
  }

  async function generateChanges() {
    setBusy('drip');
    try {
      const n = Math.max(1, parseInt(dripCount, 10) || 20);
      const r = await api.simulateDrip(n);
      push(`Generated ${n} source change(s): ${r.inserted} inserted, ${r.updated} updated, ${r.deleted} soft-deleted. Watch incremental sync pick these up on the Status tab.`);
    } catch (e) {
      push(`Failed: ${e.message}`);
    } finally { setBusy(null); }
  }

  async function toggleOutage(sink, action) {
    setBusy(`${sink}-${action}`);
    try {
      await api.simulateOutage(sink, action);
      push(`${sink} outage ${action === 'start' ? 'started' : 'stopped'} (software fault injection).`);
    } catch (e) {
      push(`Failed: ${e.message}`);
    } finally { setBusy(null); }
  }

  const esDown = status?.chaos?.es_down;
  const mqDown = status?.chaos?.mq_down;

  return (
    <div>
      <p className="muted">
        These simulate failures WITHOUT touching Docker (a software fault-injection flag the
        pipeline checks before each write) — safe to click repeatedly during a live demo.
        The gate script (<code>verify.sh</code>) additionally does a real <code>docker kill</code> /
        <code>docker stop</code> for the strongest form of proof; that's not exposed here since it
        would take down the whole container, not just this simulated path.
      </p>

      <div className="grid">
        <div className="card">
          <h3>Corrupt record</h3>
          <p className="muted">Inserts rows with a non-numeric <code>amount</code>, which Elasticsearch's strict mapping rejects — drives gate G4.</p>
          <div className="toolbar">
            <input type="number" min="1" max="100" value={corruptCount} onChange={(e) => setCorruptCount(e.target.value)} style={{ width: 70 }} />
            <button className="btn" disabled={busy === 'corrupt'} onClick={injectCorrupt}>Inject corrupt rows</button>
          </div>
        </div>

        <div className="card">
          <h3>Generate source changes</h3>
          <p className="muted">Applies a burst of random inserts/updates/soft-deletes directly to Postgres — gives incremental sync something new to pick up right now, without waiting for the CLI drip.</p>
          <div className="toolbar">
            <input type="number" min="1" max="500" value={dripCount} onChange={(e) => setDripCount(e.target.value)} style={{ width: 70 }} />
            <button className="btn" disabled={busy === 'drip'} onClick={generateChanges}>Generate changes</button>
          </div>
        </div>

        <div className="card">
          <h3>Elasticsearch outage {esDown && <span className="pill on">SIMULATED DOWN</span>}</h3>
          <p className="muted">Pipeline writes will fail with a connection error and retry with backoff until you stop it.</p>
          <div className="toolbar">
            <button className="btn danger" disabled={busy === 'elasticsearch-start' || esDown} onClick={() => toggleOutage('elasticsearch', 'start')}>Start outage</button>
            <button className="btn secondary" disabled={busy === 'elasticsearch-stop' || !esDown} onClick={() => toggleOutage('elasticsearch', 'stop')}>Stop outage</button>
          </div>
        </div>

        <div className="card">
          <h3>RabbitMQ outage {mqDown && <span className="pill on">SIMULATED DOWN</span>}</h3>
          <p className="muted">Event publishing will fail and retry with backoff until you stop it.</p>
          <div className="toolbar">
            <button className="btn danger" disabled={busy === 'rabbitmq-start' || mqDown} onClick={() => toggleOutage('rabbitmq', 'start')}>Start outage</button>
            <button className="btn secondary" disabled={busy === 'rabbitmq-stop' || !mqDown} onClick={() => toggleOutage('rabbitmq', 'stop')}>Stop outage</button>
          </div>
        </div>
      </div>

      <h3 className="section-title">Activity</h3>
      <div className="card">
        {log.length === 0 && <p className="muted">No simulated actions yet.</p>}
        {log.map((l, i) => (
          <div key={i} className="row"><span className="muted">{l.t}</span><span>{l.text}</span></div>
        ))}
      </div>
    </div>
  );
}
