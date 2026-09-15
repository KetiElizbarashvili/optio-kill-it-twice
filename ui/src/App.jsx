import React, { useEffect, useState } from 'react';
import { api } from './api.js';
import StatusPanel from './StatusPanel.jsx';
import DataBrowser from './DataBrowser.jsx';
import Controls from './Controls.jsx';
import Simulate from './Simulate.jsx';

const TABS = [
  { key: 'status', label: 'Status' },
  { key: 'data', label: 'Data browser' },
  { key: 'controls', label: 'Controls & DLQ' },
  { key: 'simulate', label: 'Simulate failures' },
];

export default function App() {
  const [tab, setTab] = useState('status');
  const [status, setStatus] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    let cancelled = false;
    function poll() {
      api.status()
        .then((d) => { if (!cancelled) { setStatus(d); setErr(null); } })
        .catch((e) => { if (!cancelled) setErr(e.message); });
    }
    poll();
    const id = setInterval(poll, 2000);
    return () => { cancelled = true; clearInterval(id); };
  }, []);

  return (
    <div className="app">
      <header className="top">
        <div>
          <h1>Kill It Twice — Pipeline Console</h1>
          <div className="sub">Postgres → Elasticsearch + RabbitMQ replication</div>
        </div>
        {status && (
          <span className={`badge ${status.overall_health === 'healthy' ? 'healthy' : 'degraded'}`}>
            {status.overall_health}
          </span>
        )}
      </header>

      {err && <div className="error-box">Can't reach API: {err}</div>}

      <nav className="tabs">
        {TABS.map((t) => (
          <button key={t.key} className={tab === t.key ? 'active' : ''} onClick={() => setTab(t.key)}>
            {t.label}
          </button>
        ))}
      </nav>

      {tab === 'status' && <StatusPanel status={status} />}
      {tab === 'data' && <DataBrowser />}
      {tab === 'controls' && <Controls status={status} />}
      {tab === 'simulate' && <Simulate status={status} />}
    </div>
  );
}
