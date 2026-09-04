import React, { useCallback, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Task = { ID: string; Role: string; State: string; Host?: string; Port?: number; Updated?: string };
type Status = { framework_id: string; scheduler_connected: boolean; desired: boolean; masters: number; slaves: number; min_masters: number; min_slaves: number; warnings?: string[]; tasks: Record<string, Task> };
type Metrics = { framework_id: string; scheduler_connected: boolean; desired: boolean; total: number; running: number; staging: number; failed: number; valkey?: { available: boolean; error: string; sections: Record<string, Record<string, string | number>> } };
type ScaleNotification = { type: 'success' | 'error'; message: string; target?: number };
type Theme = 'dark' | 'light';
type View = 'overview' | 'nodes' | 'events';
type ClusterEvent = { id: string; time: string; kind: string; message: string };

const THEME_STORAGE_KEY = 'valkey-control-plane-theme';

function readStoredTheme(): Theme {
  try {
    return window.localStorage.getItem(THEME_STORAGE_KEY) === 'light' ? 'light' : 'dark';
  } catch {
    return 'dark';
  }
}

function ThemeIcon({ theme }: { theme: Theme }) {
  return theme === 'dark' ? (
    <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20.5 15.2A8.5 8.5 0 0 1 8.8 3.5 8.5 8.5 0 1 0 20.5 15.2Z" /></svg>
  ) : (
    <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="4" /><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32 1.41 1.41M2 12h2m16 0h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41" /></svg>
  );
}

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
  const [theme, setTheme] = useState<Theme>(readStoredTheme);
  const [status, setStatus] = useState<Status | null>(null);
  const [metrics, setMetrics] = useState<Metrics | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [masterScaleNotification, setMasterScaleNotification] = useState<ScaleNotification | null>(null);
  const [slaveScaleNotification, setSlaveScaleNotification] = useState<ScaleNotification | null>(null);
  const [view, setView] = useState<View>('overview');
  const [events, setEvents] = useState<ClusterEvent[]>([]);

  useEffect(() => {
    try {
      window.localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
      // Storage can be disabled by the browser; the in-memory preference still works.
    }
  }, [theme]);

  const load = useCallback(async () => {
    try {
      const statusData = await api<Status>('/api/status');
      setStatus(statusData);
      const observedAt = new Date().toISOString();
      const observedEvents: ClusterEvent[] = [];
      statusData.warnings?.forEach(warning => observedEvents.push({ id: `warning-${warning}`, time: observedAt, kind: 'WARNING', message: warning }));
      Object.values(statusData.tasks).forEach(task => observedEvents.push({ id: `${task.ID}-${task.State}`, time: task.Updated || observedAt, kind: task.State.replace('TASK_', ''), message: `${task.Role} on ${task.Host || 'Mesos Agent'} is ${task.State.replace('TASK_', '').toLowerCase()}` }));
      setEvents(previous => {
        const merged = [...observedEvents, ...previous].filter((event, index, all) => all.findIndex(candidate => candidate.id === event.id) === index);
        return merged.slice(0, 30);
      });
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

  const schedulerConnected = status?.scheduler_connected ?? false;
  const desired = schedulerConnected && (status?.desired ?? false);
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
  // The denominator must include every live task, not only TASK_RUNNING.
  // Redis-backed scheduler state can contain nodes that are still staging or
  // starting; those nodes are already part of the cluster even though they
  // are not counted as active yet.
  const metricsTotal = desired ? (metrics?.total ?? liveTasks.length) : 0;
  const metricsStaging = desired ? (metrics?.staging ?? 0) : 0;
  const metricsFailed = desired ? (metrics?.failed ?? 0) : 0;

  useEffect(() => {
    if (masterScaleNotification?.type === 'success' && masterScaleNotification.target === actualMasters) {
      setMasterScaleNotification(null);
    }
    if (slaveScaleNotification?.type === 'success' && slaveScaleNotification.target === actualSlaves) {
      setSlaveScaleNotification(null);
    }
  }, [actualMasters, actualSlaves, masterScaleNotification, slaveScaleNotification]);

  async function adjustSlaves(delta: number) {
    if (!status) return;
    const newSlaves = status.slaves + delta;
    if (newSlaves < status.min_slaves) return;
    setBusy(true);
    setError('');
    setSlaveScaleNotification(null);
    try {
      await api<void>('/api/scale', {
        method: 'POST',
        body: JSON.stringify({ slaves: newSlaves })
      });
      setSlaveScaleNotification({ type: 'success', target: newSlaves, message: `Scaling slaves request accepted. Current target: ${newSlaves}` });
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Scaling request failed');
      setSlaveScaleNotification({ type: 'error', message: 'Scaling slaves request failed' });
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
    setMasterScaleNotification(null);
    try {
      await api<void>('/api/scale', { method: 'POST', body: JSON.stringify({ masters: newMasters }) });
      setMasterScaleNotification({ type: 'success', target: newMasters, message: `Scaling masters request accepted. Current target: ${newMasters}` });
      await load();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Master scaling request failed');
      setMasterScaleNotification({ type: 'error', message: 'Master scaling request failed' });
    } finally {
      setBusy(false);
    }
  }

  const metricSections = metrics?.valkey?.sections ?? {};

  return <div className="shell" data-theme={theme}>
    <aside>
      <div className="brand"><ValkeyClusterMark /><div><b>VALKEY</b><small>CONTROL PLANE</small></div></div>
      <nav aria-label="Primary navigation">
        {([['overview', '◈ Overview'], ['nodes', '◌ Nodes'], ['events', '⌁ Events']] as const).map(([key, label]) => <a key={key} href={`#${key}`} className={view === key ? 'active' : ''} onClick={(event) => { event.preventDefault(); setView(key); }}>{label}</a>)}
      </nav>
      <div className="sidefoot"><span className="pulse" /> Mesos connected</div>
    </aside>
    <main>
      <header><div><p className="eyebrow">PLATFORM / MESOS</p><h1>{view === 'overview' ? 'Cluster Overview' : view === 'nodes' ? 'Cluster Nodes' : 'Cluster Events'}</h1></div><div className="header-actions"><button className="theme-toggle" type="button" aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`} title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`} onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}><ThemeIcon theme={theme} /></button><div className="header-meta"><span className="live-dot" /> LIVE <span className="divider" /> {new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })}</div></div></header>
      {error && <div className="alert" role="alert">⚠ {error}</div>}

      {status?.warnings?.map((warning, index) => <div className="alert scheduler-warning" role="alert" key={`${warning}-${index}`}>⚠ {warning}</div>)}
      {view === 'overview' && <>
          <section className="hero"><div><span className="eyebrow">VALKEY CLUSTER</span><h2>{!clusterKnown ? 'Loading…' : !schedulerConnected ? 'Unavailable' : desired ? 'Running' : 'Stopped'}</h2><p>{!schedulerConnected && clusterKnown ? 'Scheduler is not connected to Mesos; persisted state is not considered live.' : 'Managed by the Valkey Scheduler on Apache Mesos.'}</p></div><div className="hero-stat"><strong>{metricsRunning}<i> / {metricsTotal}</i></strong><span>active nodes</span></div></section>
          <div className="grid">
            <article className="card metric"><span className="label">HEALTH</span><strong>{status && schedulerConnected ? `${metricsRunning} / ${metricsRunning}` : '—'}</strong><span className={schedulerConnected ? 'good' : 'warning'}>● {schedulerConnected ? 'Mesos Connected' : 'Mesos Disconnected'}</span></article>
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
          <section className="card control"><div><span className="label">SCALING</span><h3>Cluster size</h3><p>This Valkey model requires at least one master and {status?.min_slaves ?? '—'} slave.</p></div><div className="scale"><div className="scale-group"><span className="slave-count" aria-label="Master count">Masters: {actualMasters}</span>{masterScaleNotification && <div className={`scale-feedback ${masterScaleNotification.type}`} role={masterScaleNotification.type === 'error' ? 'alert' : 'status'} aria-live="polite">{masterScaleNotification.type === 'success' ? '✓ ' : '✗ '}{masterScaleNotification.message}</div>}<div className="scale-buttons"><button className="secondary" aria-label="Decrease master count" onClick={() => void adjustMasters(-1)} disabled={busy || !clusterKnown || !desired || !status || status.masters <= 1}>− Master</button><button className="secondary" aria-label="Increase master count" onClick={() => void adjustMasters(1)} disabled={busy || !clusterKnown || !desired}>+ Master</button></div></div><div className="scale-group"><span className="slave-count" aria-label="Slave count">Slaves: {actualSlaves}</span>{slaveScaleNotification && <div className={`scale-feedback ${slaveScaleNotification.type}`} role={slaveScaleNotification.type === 'error' ? 'alert' : 'status'} aria-live="polite">{slaveScaleNotification.type === 'success' ? '✓ ' : '✗ '}{slaveScaleNotification.message}</div>}<div className="scale-buttons"><button className="secondary" aria-label="Decrease slave count" onClick={() => void adjustSlaves(-1)} disabled={busy || !clusterKnown || !desired || !status || status.slaves <= status.min_slaves}>− Slave</button><button className="secondary" aria-label="Increase slave count" onClick={() => void adjustSlaves(1)} disabled={busy || !clusterKnown || !desired}>+ Slave</button></div></div></div></section>
      </>}
      {view === 'overview' && <section className="server-map-section"><div className="nodes-head"><div><span className="label">SERVER MAP</span><h3>All server activity</h3></div><span className="refresh">Color indicates task state</span></div><div className="server-heatmap">{liveTasks.length ? liveTasks.map(task => <div className={`server-cell ${task.State.toLowerCase().replace('task_', '')}`} key={task.ID}><span className="server-cell-role">{task.Role}</span><strong>{task.Host || 'Mesos Agent'}</strong><small>{task.State.replace('TASK_', '')}</small></div>) : <div className="empty card">No live servers. The scheduler is waiting for Mesos Offers.</div>}</div></section>}
      {view === 'nodes' && <><section className="nodes-head"><div><span className="label">INSTANCES</span><h3>Valkey Nodes</h3></div><span className="refresh">Refreshes every 5 seconds</span></section><div className="node-grid">{liveTasks.length ? liveTasks.map(task => <article className="node card" key={task.ID}><div className="node-top"><ValkeyClusterMark /><span className={`badge ${task.State.toLowerCase().replace('task_', '')}`}>{task.State.replace('TASK_', '')}</span></div><h3>{task.Role}</h3><p>{task.Host || 'Mesos Agent'}</p><div className="node-data"><span>PORT <b>{task.Port ?? '—'}</b></span><span>UPDATED <b>{task.Updated ? new Date(task.Updated).toLocaleTimeString('en-US') : '—'}</b></span></div></article>) : <div className="empty card">No live nodes. The scheduler is waiting for Mesos Offers.</div>}</div></>}
      {view === 'events' && <section className="events-section"><div className="nodes-head"><div><span className="label">ACTIVITY LOG</span><h3>Scheduler events</h3></div><span className="refresh">Observed by dashboard</span></div><div className="event-list card">{events.length ? events.map(event => <div className="event-row" key={event.id}><time>{new Date(event.time).toLocaleTimeString('en-US')}</time><span className={`event-kind ${event.kind.toLowerCase()}`}>{event.kind}</span><p>{event.message}</p></div>) : <div className="empty">No events observed yet.</div>}</div></section>}
    </main>
  </div>;
}

createRoot(document.getElementById('root')!).render(<React.StrictMode><App /></React.StrictMode>);
