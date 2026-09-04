# Valkey Mesos Framework

Eigenständiges Apache-Mesos-v1-Framework, das einen Valkey-Master und konfigurierbare Valkey-Replikas als Docker-Tasks startet. Die Scheduler-Anbindung verwendet die typisierte v1-API aus `github.com/m3scluster/clusterd-go`; HTTP-Steuerung, Umgebungsvariablen und Redis-State bleiben lokal im Framework.

## Eigenschaften

- Mesos-v1-`SUBSCRIBE` über `/api/v1/scheduler`
- RecordIO-Decoding für den langlebigen Event-Stream
- Übernahme und Verwendung von `Mesos-Stream-Id`
- `OFFERS`, `SUBSCRIBED` und `UPDATE`-Events
- Docker-`LAUNCH` mit expliziten CPU- und RAM-Ressourcen
- standardmäßig ein Master plus zwei Slaves; Master und Slaves sind getrennt skalierbar
- Mesos-CNI-Netzwerk ohne externe Portfreigaben; standardmäßig `MESOS_CNI=weave`
- `/healthz`, `/api/status`, `/api/start`, `/api/stop`, `/api/scale`
- `MESOS_DRY_RUN=true` für lokale Prüfung ohne Mesos
- Redis-State mit konfigurierbarem Server und DB; standardmäßig `redis.weave.local:6379`, DB `10`
- Logrus-Meldungen mit `LOG_LEVEL=debug|info|error`, Standard: `info`

## Bauen und lokal prüfen

```bash
go test ./...
go vet ./...
go build -o valkey-mesos-framework .
MESOS_DRY_RUN=true LISTEN_ADDR=127.0.0.1:10001 ./valkey-mesos-framework
curl http://127.0.0.1:10001/healthz
curl -X POST http://127.0.0.1:10001/api/start
curl http://127.0.0.1:10001/api/status
```

## Mesos-Konfiguration

```bash
export MESOS_MASTER=leader.mesos:5050
export MESOS_USERNAME=mesos
export MESOS_PASSWORD='aus sicherer Laufzeitumgebung'
export REDIS_SERVER=redis.weave.local:6379
export REDIS_DB=10
# optional: export REDIS_PASSWORD='aus sicherer Laufzeitumgebung'
export MESOS_CNI=weave
export MESOS_DOMAIN=mesos
# Standard ist: master.<MESOS_DOMAIN> (ohne FRAMEWORK_NAME)
# optional: export VALKEY_MASTER_HOST=master.mesos
export MESOS_ROLE='*'
export MESOS_CHECKPOINT=true
export VALKEY_MASTERS=1
export VALKEY_SLAVES=2
export VALKEY_IMAGE=valkey/valkey:8-alpine
export VALKEY_CPU=0.2
export VALKEY_MEMORY_MB=256
./valkey-mesos-framework
```

Checkpointing ist standardmäßig aktiviert und wird mit `MESOS_CHECKPOINT=false` nur für explizite Entwicklungs- oder Kompatibilitätstests abgeschaltet. Dadurch kann Mesos den Framework-Zustand beim Scheduler-Failover wiederherstellen.

`MESOS_TLS_INSECURE=true` ist nur für Entwicklungscluster mit selbstsigniertem Zertifikat vorgesehen. Für Produktion soll der Transport mit einer CA-Datei erweitert bzw. vor einem TLS-terminierenden Proxy betrieben werden.

## Wichtige Einschränkung

Das Framework weist Tasks auf Mesos-Agenten zu. Wenn `MESOS_CNI` gesetzt ist, wird das konfigurierte Mesos-CNI-Netzwerk angefordert; bei leerem Wert wird keine CNI-Netzwerk-Information gesendet und Docker verwendet sein Standardnetzwerk. Für die Namensauflösung wird Mesos-DNS verwendet: Der Master wird als `master.<MESOS_DOMAIN>` veröffentlicht; Container-Hostnamen werden als `<task>.<MESOS_DOMAIN>` gesetzt. Der Framework-Name ist nicht Bestandteil dieser Hostnamen. Alternativ kann `VALKEY_MASTER_HOST` gesetzt werden. Die Replikas verwenden diesen Namen auf Port `6379`. Mesos meldet die Anwendung erst nach `TASK_RUNNING` als gestartet; ein akzeptiertes Offer allein ist kein Healthcheck.

`POST /api/scale` akzeptiert `{"masters": N}` und/oder `{"slaves": N}`. Beide Werte werden unabhängig reconciliert; beim Verkleinern beendet das Framework überschüssige laufende Tasks über die native Mesos-KILL-API, bevor der neue Zielwert persistiert wird. Mindestens ein Master und ein Slave bleiben erhalten.

`/api/stop` beendet aktuell die gewünschte Bereitstellung und verhindert weitere Starts. Bereits laufende Tasks werden dabei nicht automatisch gekillt.

## Referenz

Die Referenz implementiert einen generischen Compose-Adapter und nutzt ebenfalls Framework-Konfiguration über Umgebungsvariablen, Docker-Task-Starts und Mesos-Angebote. Dieses Projekt hält den Umfang bewusst auf einen Valkey-Cluster begrenzt.
