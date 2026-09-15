import React, { useEffect, useState } from 'react';
import { api } from './api.js';

export default function Controls({ status }) {
  const [dlq, setDlq] = useState({ total: 0, rows: [] });
  const [busy, setBusy] = useState(null);
  const [msg, setMsg] = useState(null);

  function refreshDlq() {
    api.dlq('pending').then(setDlq).catch(() => {});
  }
  useEffect(() => {
    refreshDlq();
    const id = setInterval(refreshDlq, 4000);
    return () => clearInterval(id);
  }, []);

  async function toggle(mode, action) {
    setBusy(`${mode}-${action}`);
    try { await api.control(mode, action); } finally { setBusy(null); }
  }

  async function replay(id) {
    setBusy(`replay-${id}`);
    setMsg(null);
    try {
      await api.replayDlq(id);
      setMsg({ ok: true, text: `Row ${id} replayed successfully.` });
      refreshDlq();
    } catch (e) {
      setMsg({ ok: false, text: `Row ${id}: ${e.message}` });
    } finally {
      setBusy(null);
    }
  }

  return (
    <div>
      <h3 className="section-title" style={{ marginTop: 0 }}>Pipeline control</h3>
      <div className="grid">
        <div className="card">
          <h3>Backfill — {status?.backfill?.status || '…'}</h3>
          <div className="toolbar">
            <button className="btn secondary" disabled={busy === 'backfill-pause'} onClick={() => toggle('backfill', 'pause')}>Pause</button>
            <button className="btn" disabled={busy === 'backfill-resume'} onClick={() => toggle('backfill', 'resume')}>Resume</button>
          </div>
        </div>
        <div className="card">
          <h3>Incremental — {status?.incremental?.status || '…'}</h3>
          <div className="toolbar">
            <button className="btn secondary" disabled={busy === 'incremental-pause'} onClick={() => toggle('incremental', 'pause')}>Pause</button>
            <button className="btn" disabled={busy === 'incremental-resume'} onClick={() => toggle('incremental', 'resume')}>Resume</button>
          </div>
        </div>
      </div>

      <h3 className="section-title">Dead-letter queue ({dlq.total} pending)</h3>
      {msg && <div className={msg.ok ? 'card' : 'error-box'} style={msg.ok ? { borderColor: '#2f5', marginBottom: 10 } : {}}>{msg.text}</div>}
      <div className="table-wrap">
        <table>
          <thead><tr><th>DLQ id</th><th>Source id</th><th>Sink</th><th>Error</th><th>Attempts</th><th></th></tr></thead>
          <tbody>
            {dlq.rows.map((r) => (
              <tr key={r.id}>
                <td>{r.id}</td>
                <td>{r.source_record_id}</td>
                <td>{r.sink}</td>
                <td style={{ maxWidth: 420, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }} title={r.error}>{r.error}</td>
                <td>{r.attempt_count}</td>
                <td><button className="btn" disabled={busy === `replay-${r.id}`} onClick={() => replay(r.id)}>Replay</button></td>
              </tr>
            ))}
            {dlq.rows.length === 0 && <tr><td colSpan={6} className="muted">No pending DLQ entries.</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}
