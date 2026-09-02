# Valkey Mesos Control Plane

Zweiteilige Referenzimplementierung:

- `backend/app.go`: Prozessstart, HTTP-Server und Lifecycle.
- `backend/init.go`: zentrale Initialisierung aller Environment-Variablen über `init()`.
- `backend/main.go`: API, Domänenmodelle und Mesos-Abstraktion.
- `frontend/`: React/Vite-Administrationsoberfläche mit Live-Polling.

## Start

```bash
make run
```

Das startet Backend auf `http://localhost:8080` und Frontend auf `http://localhost:5173`. Für einen sicheren lokalen Test läuft der Backend-Scheduler standardmäßig in `DRY_RUN=true`; Nodes und Skalierung sind dann deterministisch sichtbar, werden aber nicht auf Mesos gestartet.

Für den Testcluster können Credentials ausschließlich über die Shell gesetzt werden:

```bash
MESOS_MASTER=devtest.lab.internal:5050 MESOS_USERNAME=mesos MESOS_PASSWORD="$MESOS_PASSWORD" DRY_RUN=true make run
```

`DRY_RUN=false` aktiviert die explizite Produktions-Seam. Die Mesos-v1-Framework-Registrierung/Streaming-Integration ist noch bewusst nicht aktiviert; der Backend-Start bricht bei einer Skalierung mit einer erklärenden Fehlermeldung ab, statt einen erfolgreichen Deployment-Status vorzutäuschen.

## API

- `GET /api/health`
- `GET /api/cluster`
- `PUT /api/cluster/scale` mit `{ "nodes": 3..100 }`
- `GET /api/metrics`

Die Metrics melden im Dry-Run explizit `unknown_no_valkey_endpoint`, weil keine echten Valkey-Prozesse laufen. Sobald Mesos-Task-Status und Endpoints integriert sind, fragt der Backend-Metrics-Collector Valkey `INFO` über TCP ab.

## Grundlage

Die Scheduler-Struktur orientiert sich an [m3scluster/compose](https://github.com/m3scluster/compose): Framework-/Mesos-Client hinter Interface, Reconciliation als Quelle für die gewünschte Task-Anzahl, sowie offer-/task-orientiertes Scheduling. Die externe Integration bleibt von der lokalen Dry-Run-Implementierung getrennt.
