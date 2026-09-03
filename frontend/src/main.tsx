import React, { useCallback, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Task = { ID: string; Role: string; State: string; Host?: string; Port?: number; Updated?: string };
type Status = { framework_id: string; desired: boolean; masters: number; slaves: number; min_masters: number; min_slaves: number; warnings?: string[]; tasks: Record<string, Task> };
type Metrics = { framework_id: string; desired: boolean; total: number; running: number; staging: number; failed: number; valkey?: { available: boolean; error: string; sections: Record<string, Record<string, string | number>> } };

async function api<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...init,
    headers: { 'Content-Type': 'application/json', ...(init?.headers || {}) },
  });
  const body = await response.text();
  if (!response.ok) throw new Error(body || `HTTP ${response.status}`);
  return (body ? JSON.parse(body) : undefined) as T;
}

function ValkeyClusterMark() {
  return <svg className="cluster-mark" viewBox="0 0 44 36" role="img" aria-label="ClusterD Valkey mark">
    <path className="mark-network" d="M5 18 13 7l9 11 9-11 8 11-8 11-9-11-9 11Z" />
    <path className="mark-chevron" d="m10 18 7-7 7 7-7 7Z" />
    <path className="mark-chevron mark-chevron-second" d="m24 18 7-7 7 7-7 7Z" />
    <circle className="mark-node" cx="5" cy="18" r="2.4" /><circle className="mark-node" cx="13" cy="7" r="2.4" /><circle className="mark-node" cx="13" cy="29" r="2.4" />
  </svg>;
}

function App() {
  const [status, setStatus] = useState<Status | null>(null);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [scaleNotification, setScaleNotification] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  const load = useCallback(async () => {
    try {
      const statusData = await api<Status>('/api/status');
      setStatus(statusData);
      try {
        const metricData = await api<Metrics>('/api/metrics');
        setMetrics(metricData);
      } catch (err) {
        setMetrics(null);
      }
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Backend not reachable');
    }
  }, []);

  useEffect(() => {
    void load();
    const id = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(id);
  }, [load]);

  async function stopCluster() {
    setBusy(true);
    setError('');
    try {
      await api<void>('/api/stop', { method: 'POST' });
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Request failed');
    } finally {
      setBusy(false);
    }
  }

  async function startCluster() {
    setBusy(true);
    setError('');
    try {
      await api<void>('/api/start', { method: 'POST' });
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Request failed');
    } finally {
      setBusy(false);
    }
  }

  const desired = status?.desired ?? false;
  const clusterKnown = status !== null;
  // The scheduler status is authoritative. Terminal records are history, not
  // live nodes or part of the active-node denominator.
  const liveTasks = desired && status ? Object.values(status.tasks).filter(task => ['TASK_RUNNING', 'TASK_STAGING', 'TASK_STARTING', 'TASK_UNKNOWN'].includes(task.State)) : [];
  const tasks = desired && status ? Object.values(status.tasks).filter(task => task.State === 'TASK_RUNNING') : [];
  const running = tasks.length;

  const metricsRunning = desired ? (metrics?.running ?? running) : 0;
  // Scaling changes the scheduler's desired target immediately, but a server
  // only exists for the dashboard once its task is actually running. Keep all
  // server-count displays tied to the observed task states instead of the
  // desired values returned by /api/status.
  const actualMasters = tasks.filter(task => task.Role === 'master' || task.Role.startsWith('master-')).length;
  const actualSlaves = tasks.filter(task => task.Role.startsWith('slave-')).length;
  const metricsTotal = desired ? running : 0;
  const metricsStaging = desired ? (metrics?.staging ?? 0) : 0;
  const metricsFailed = desired ? (metrics?.failed ?? 0) : 0;

  async function adjustSlaves(delta: number) {
    if (!status) return;
    setBusy(true);
    setError('');
    setScaleNotification(null);
    try {
      const newSlaves = status.slaves + delta;
      if (newSlaves < status.min_slaves) return;
      await api<void>('/api/scale', {
        method: 'POST',
        body: JSON.stringify({ slaves: newSlaves })
      });
      setScaleNotification({type: 'success', message: `Scaling slaves request accepted. Current target: ${newSlaves}`});
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Scaling request failed');
      setScaleNotification({ type: 'error', message: 'Scaling slaves request failed' });
    } finally {
      setBusy(false);
    }
  }

  async function adjustMasters(delta: number) {
    if (!status) return;
    const newMasters = status.masters + delta;
    if (newMasters < 1) return;
    setBusy(true);
    setError('');
    setScaleNotification(null);
    try {
      await api<void>('/api/scale', { method: 'POST', body: JSON.stringify({ masters: newMasters }) });
      setScaleNotification({type: 'success', message: `Scaling masters request accepted. Current target: ${newMasters}`});
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Master scaling request failed');
      setScaleNotification({ type: 'error', message: 'Scaling masters request failed' });
    } finally {
      setBusy(false);
    }
  }

  const metricSections = metrics?.valkey?.sections ?? {};

  return <div className="shell">
    <aside>
      <div className="brand"><ValkeyClusterMark /><div><b>VALKEY</b><small>CONTROL PLANE</small></div></div>
      <nav><a className="active">◈ Overview</a><a>◌ Nodes</a><a>⌁ Events</a></nav>
      <div className="sidefoot"><span className="pulse" /> Mesos connected</div>
    </aside>
    <main>
      <header><div><p className="eyebrow">PLATFORM / MESOS</p><h1>Cluster Overview</h1></div><div className="header-meta"><span className="live-dot" /> LIVE <span className="divider" /> {new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}</div></header>
      {error && <div className="alert" role="alert">⚠ {error}</div>}
      {scaleNotification && (
        <div className={`scale-feedback ${scaleNotification.type}`} role={scaleNotification.type === 'error' ? 'alert' : 'status'} aria-live="polite">
          {scaleNotification.type === 'success' ? '✓ ' : '✗ '}
          {scaleNotification.message}
        </div>
      )}
      {status?.warnings?.map((warning, index) => <div className="alert scheduler-warning" role="alert" key={`${warning}-${index}`}>⚠ {warning}</div>)}
      <section className="hero"><div><span className="eyebrow">VALKEY CLUSTER</span><h2>{!clusterKnown ? 'Loading…' : desired ? 'Running' : 'Stopped'}</h2><p>Managed by the Valkey Scheduler on Apache Mesos.</p></div><div className="hero-stat"><strong>{metricsRunning}<i> / {metricsTotal}</i></strong><span>active nodes</span></div></section>
      <div className="grid">
        <article className="card metric"><span className="label">HEALTH</span><strong>{status ? `${metricsRunning} / ${metricsRunning}` : '—'}</strong><span className="good">● Running Nodes</span></article>
        <article className="card metric"><span className="label">FRAMEWORK ID</span><strong className="compact">{status?.framework_id || 'Not registered'}</strong><span className="muted">scheduler registration</span></article>
        <article className="card metric"><span className="label">DESIRED STATE</span><strong>{status ? (desired ? 'ON' : 'OFF') : '—'}</strong><span className="muted">controlled by scheduler</span></article>
      </div>
      <div className="grid node-metrics">
        <article className="card metric"><span className="label">TOTAL SERVERS</span><strong>{status ? metricsTotal : '—'}</strong><span className="muted">tracked Valkey tasks</span></article>
        <article className="card metric"><span className="label">STARTING</span><strong>{status ? metricsStaging : '—'}</strong><span className="muted">waiting for Mesos</span></article>
        <article className="card metric"><span className="label">FAILED / LOST</span><strong className={metricsFailed ? 'warning' : ''}>{status ? metricsFailed : '—'}</strong><span className="muted">requires reconciliation</span></article>
      </div>
      <section className="metrics-section"><div className="nodes-head"><div><span className="label">VALKEY SDK</span><h3>Live server metrics</h3></div><span className="refresh">INFO via Valkey client</span></div>{metrics?.valkey?.available ? <div className="metric-sections">{Object.entries(metricSections).map(([section, values]) => <article className="card info-card" key={section}><span className="label">{section.toUpperCase()}</span>{Object.entries(values).slice(0, 6).map(([key, value]) => <div className="info-row" key={key}><span title={key}>{key}</span><strong>{typeof value === 'number' ? value.toLocaleString('en-US') : value}</strong>{typeof value === 'number' && <span className="metric-bar"><i style={{ width: `${Math.min(100, Math.max(4, Math.abs(value) % 100))}%` }} /></span>}</div>)}</article>)}</div> : <div className="card metrics-unavailable">Valkey metrics unavailable{metrics?.valkey?.error ? `: ${metrics.valkey.error}` : '.'}</div>}</section>
      <section className="card control"><div><span className="label">DEPLOYMENT</span><h3>Scheduler-managed cluster</h3><p>The scheduler starts and reconciles the Valkey deployment. The dashboard is observing it or requesting a stop.</p></div><div className="scale"><button className="primary" onClick={() => void startCluster()} disabled={busy || !clusterKnown || desired}>{busy ? 'Starting…' : 'Start cluster'}</button><button className="danger" onClick={() => void stopCluster()} disabled={busy || !clusterKnown || !desired}>{busy ? 'Stopping…' : 'Stop cluster'}</button></div></section>
      <section className="card control"><div><span className="label">SCALING</span><h3>Cluster size</h3><p>This Valkey model requires at least one master and {status?.min_slaves ?? '—'} slave.</p></div><div className="scale"><span className="slave-count" aria-label="Master count">Masters: {actualMasters}</span><button className="secondary" aria-label="Decrease master count" onClick={() => void adjustMasters(-1)} disabled={busy || !clusterKnown || !desired || !status || status.masters <= 1}>− Master</button><button className="secondary" aria-label="Increase master count" onClick={() => void adjustMasters(1)} disabled={busy || !clusterKnown || !desired}>+ Master</button><span className="slave-count" aria-label="Slave count">Slaves: {actualSlaves}</span><button className="secondary" aria-label="Decrease slave count" onClick={() => void adjustSlaves(-1)} disabled={busy || !clusterKnown || !desired || !status || status.slaves <= status.min_slaves}>− Slave</button><button className="secondary" aria-label="Increase slave count" onClick={() => void adjustSlaves(1)} disabled={busy || !clusterKnown || !desired}>+ Slave</button></div></section>
      <section className="nodes-head"><div><span className="label">INSTANCES</span><h3>Valkey Nodes</h3></div><span className="refresh">Refreshes every 5 seconds</span></section>
      <div className="node-grid">{tasks.length ? tasks.map(task => <article className="node card" key={task.ID}><div className="node-top"><ValkeyClusterMark /><span className="badge running">RUNNING</span></div><h3>{task.Role}</h3><p>{task.Host || 'Mesos Agent'}</p><div className="node-data"><span>PORT <b>{task.Port ?? '—'}</b></span><span>UPDATED <b>{task.Updated ? new Date(task.Updated).toLocaleTimeString('en-US') : '—'}</b></span></div></article>) : <div className="empty card">No running nodes. The scheduler is waiting for Mesos Offers.</div>}</div>
    </main>
  </div>;
}

createRoot(document.getElementById('root')!).render(<React.StrictMode><App /></React.StrictMode>);
