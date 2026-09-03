import React, { useCallback, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Task = { ID: string; Role: string; State: string; Host?: string; Port?: number; Updated?: string };
type Status = { framework_id: string; desired: boolean; tasks: Record<string, Task> };

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
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    try {
      setStatus(await api<Status>('/api/status'));
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
  // The scheduler status is authoritative. Terminal/staged records are not live nodes.
  const tasks = desired && status ? Object.values(status.tasks).filter(task => task.State === 'TASK_RUNNING') : [];
  const running = tasks.length;

  return <div className="shell">
    <aside>
      <div className="brand"><ValkeyClusterMark /><div><b>VALKEY</b><small>CONTROL PLANE</small></div></div>
      <nav><a className="active">◈ Overview</a><a>◌ Nodes</a><a>⌁ Events</a></nav>
      <div className="sidefoot"><span className="pulse" /> Mesos connected</div>
    </aside>
    <main>
      <header><div><p className="eyebrow">PLATFORM / MESOS</p><h1>Cluster Overview</h1></div><div className="header-meta"><span className="live-dot" /> LIVE <span className="divider" /> {new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}</div></header>
      {error && <div className="alert" role="alert">⚠ {error}</div>}
      <section className="hero"><div><span className="eyebrow">VALKEY CLUSTER</span><h2>{!clusterKnown ? 'Loading…' : desired ? 'Running' : 'Stopped'}</h2><p>Managed by the Valkey Scheduler on Apache Mesos.</p></div><div className="hero-stat"><strong>{running}<i> / {desired && status ? Object.keys(status.tasks).length : 0}</i></strong><span>active nodes</span></div></section>
      <div className="grid">
        <article className="card metric"><span className="label">HEALTH</span><strong>{status ? `${running} / ${running}` : '—'}</strong><span className="good">● Running Nodes</span></article>
        <article className="card metric"><span className="label">FRAMEWORK ID</span><strong className="compact">{status?.framework_id || 'Not registered'}</strong><span className="muted">scheduler registration</span></article>
        <article className="card metric"><span className="label">DESIRED STATE</span><strong>{status ? (desired ? 'ON' : 'OFF') : '—'}</strong><span className="muted">controlled by scheduler</span></article>
      </div>
      <section className="card control"><div><span className="label">DEPLOYMENT</span><h3>Scheduler-managed cluster</h3><p>The scheduler starts and reconciles the Valkey deployment. The dashboard only observes it or requests a stop.</p></div><div className="scale"><button className="primary" onClick={() => void startCluster()} disabled={busy || !clusterKnown || desired}>{busy ? 'Starting…' : 'Start cluster'}</button><button className="danger" onClick={() => void stopCluster()} disabled={busy || !clusterKnown || !desired}>{busy ? 'Stopping…' : 'Stop cluster'}</button></div></section>
      <section className="nodes-head"><div><span className="label">INSTANCES</span><h3>Valkey Nodes</h3></div><span className="refresh">Refreshes every 5 seconds</span></section>
      <div className="node-grid">{tasks.length ? tasks.map(task => <article className="node card" key={task.ID}><div className="node-top"><ValkeyClusterMark /><span className="badge running">RUNNING</span></div><h3>{task.Role}</h3><p>{task.Host || 'Mesos Agent'}</p><div className="node-data"><span>PORT <b>{task.Port ?? '—'}</b></span><span>UPDATED <b>{task.Updated ? new Date(task.Updated).toLocaleTimeString('en-US') : '—'}</b></span></div></article>) : <div className="empty card">No running nodes. The scheduler is waiting for Mesos Offers.</div>}</div>
    </main>
  </div>;
}

createRoot(document.getElementById('root')!).render(<React.StrictMode><App /></React.StrictMode>);
