import React, { useState } from 'react';
import { api } from './api.js';

export default function Simulate({ status }) {
  const [busy, setBusy] = useState(null);
  const [log, setLog] = useState([]);

  function push(text) {
    setLog((l) => [{ t: new Date().toLocaleTimeString(), text }, ...l].slice(0, 20));
  }

  async function injectCorrupt() {
    setBusy('corrupt');
    try {
      const r = await api.simulateCorrupt(3);
      push(`Inserted 3 corrupt rows (amount="N/A"): ids ${r.ids.join(', ')}. Watch the DLQ count on the Status tab.`);
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
          <p className="muted">Inserts 3 rows with a non-numeric <code>amount</code>, which Elasticsearch's strict mapping rejects — drives gate G4.</p>
          <button className="btn" disabled={busy === 'corrupt'} onClick={injectCorrupt}>Inject 3 corrupt rows</button>
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
