import React, { useEffect, useState } from 'react';
import { api } from './api.js';

export default function DataBrowser() {
  const [q, setQ] = useState('');
  const [status, setStatus] = useState('');
  const [country, setCountry] = useState('');
  const [page, setPage] = useState(1);
  const [data, setData] = useState({ total: 0, records: [] });
  const [selected, setSelected] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    let cancelled = false;
    const params = { page, limit: 20 };
    if (q) params.q = q;
    if (status) params.status = status;
    if (country) params.country = country;
    api.records(params).then((d) => !cancelled && setData(d)).catch((e) => !cancelled && setErr(e.message));
    return () => { cancelled = true; };
  }, [q, status, country, page]);

  useEffect(() => {
    const id = setInterval(() => {
      const params = { page, limit: 20 };
      if (q) params.q = q;
      if (status) params.status = status;
      if (country) params.country = country;
      api.records(params).then(setData).catch(() => {});
    }, 4000);
    return () => clearInterval(id);
  }, [q, status, country, page]);

  const totalPages = Math.max(1, Math.ceil(data.total / 20));

  return (
    <div>
      <div className="toolbar">
        <input placeholder="Search name / email / company…" value={q} onChange={(e) => { setPage(1); setQ(e.target.value); }} style={{ minWidth: 260 }} />
        <select value={status} onChange={(e) => { setPage(1); setStatus(e.target.value); }}>
          <option value="">All statuses</option>
          <option value="active">active</option>
          <option value="inactive">inactive</option>
          <option value="pending">pending</option>
        </select>
        <input placeholder="Country" value={country} onChange={(e) => { setPage(1); setCountry(e.target.value); }} style={{ width: 140 }} />
        <span className="muted">{data.total.toLocaleString()} matching (live index)</span>
      </div>

      {err && <div className="error-box">{err}</div>}

      <div className="table-wrap">
        <table>
          <thead>
            <tr><th>ID</th><th>Name</th><th>Email</th><th>Company</th><th>Country</th><th>Status</th><th>Amount</th><th>Updated</th></tr>
          </thead>
          <tbody>
            {data.records.map((r) => (
              <tr key={r.id} onClick={() => setSelected(r)} style={{ cursor: 'pointer' }}>
                <td>{r.id}</td>
                <td>{r.name}</td>
                <td>{r.email}</td>
                <td>{r.company}</td>
                <td>{r.country}</td>
                <td>{r.status}</td>
                <td>{r.amount}</td>
                <td>{new Date(r.updated_at).toLocaleTimeString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="toolbar" style={{ marginTop: 12 }}>
        <button className="btn secondary" disabled={page <= 1} onClick={() => setPage((p) => p - 1)}>Prev</button>
        <span className="muted">page {page} / {totalPages}</span>
        <button className="btn secondary" disabled={page >= totalPages} onClick={() => setPage((p) => p + 1)}>Next</button>
      </div>

      {selected && (
        <div className="card" style={{ marginTop: 16 }}>
          <h3>Record #{selected.id}</h3>
          <pre className="payload">{JSON.stringify(selected, null, 2)}</pre>
          <button className="btn secondary" onClick={() => setSelected(null)}>Close</button>
        </div>
      )}
    </div>
  );
}
