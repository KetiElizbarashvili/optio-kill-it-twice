import React from 'react';

function fmtNum(n) {
  if (n === null || n === undefined) return '—';
  return Number(n).toLocaleString(undefined, { maximumFractionDigits: 1 });
}

export default function StatusPanel({ status }) {
  if (!status) return <p className="muted">Loading status…</p>;
  const { source, backfill, incremental, sinks, consumer, dlq } = status;

  return (
    <div>
      <div className="grid">
        <div className="card">
          <h3>Backfill</h3>
          <div className="metric">{backfill.progress_pct}%</div>
          <div className="progress-bar"><div style={{ width: `${backfill.progress_pct}%` }} /></div>
          <div className="row"><span>Status</span><span>{backfill.status}</span></div>
          <div className="row"><span>Last id</span><span>{fmtNum(backfill.last_id)} / {fmtNum(source.max_id)}</span></div>
          <div className="row"><span>Throughput</span><span>{fmtNum(backfill.throughput_rows_per_sec)} rows/s</span></div>
        </div>

        <div className="card">
          <h3>Incremental sync</h3>
          <div className="metric">{fmtNum(incremental.lag_seconds)}s</div>
          <div className="label">lag behind wall clock</div>
          <div className="row"><span>Status</span><span>{incremental.status}</span></div>
          <div className="row"><span>Rows processed</span><span>{fmtNum(incremental.rows_processed)}</span></div>
          <div className="row"><span>Throughput</span><span>{fmtNum(incremental.throughput_rows_per_sec)} rows/s</span></div>
        </div>

        <div className="card">
          <h3>DLQ</h3>
          <div className="metric" style={{ color: dlq.pending_count > 0 ? '#fbbf24' : undefined }}>
            {fmtNum(dlq.pending_count)}
          </div>
          <div className="label">pending failures</div>
        </div>

        <div className="card">
          <h3>Source</h3>
          <div className="metric">{fmtNum(source.total_records)}</div>
          <div className="label">live rows in Postgres</div>
        </div>
      </div>

      <h3 className="section-title">Sinks &amp; consumer</h3>
      <div className="grid">
        <div className="card">
          <h3>Elasticsearch</h3>
          <div className="row">
            <span>Health</span>
            <span className={`badge ${sinks.elasticsearch.ok ? 'healthy' : 'down'}`}>{sinks.elasticsearch.status || 'unknown'}</span>
          </div>
        </div>

        <div className="card">
          <h3>RabbitMQ</h3>
          <div className="row"><span>Health</span><span className={`badge ${sinks.rabbitmq.ok ? 'healthy' : 'down'}`}>{sinks.rabbitmq.ok ? 'up' : 'down'}</span></div>
          <div className="row"><span>Queue depth</span><span>{fmtNum(sinks.rabbitmq.queue?.messages)}</span></div>
          <div className="row"><span>Consumers</span><span>{fmtNum(sinks.rabbitmq.queue?.consumers)}</span></div>
          <div className="row"><span>DLQ (broker)</span><span>{fmtNum(sinks.rabbitmq.dlq_queue?.messages)}</span></div>
        </div>

        <div className="card">
          <h3>Independent consumer</h3>
          <div className="row"><span>Reachable</span><span className={`badge ${consumer.reachable ? 'healthy' : 'down'}`}>{consumer.reachable ? 'yes' : 'no'}</span></div>
          <div className="row"><span>Processed (unique)</span><span>{fmtNum(consumer.processed_total)}</span></div>
          <div className="row"><span>Duplicates skipped</span><span>{fmtNum(consumer.duplicates_skipped_total)}</span></div>
        </div>
      </div>
    </div>
  );
}
